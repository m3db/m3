package storage

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/sharding"
)

var (
	// errDatabaseAlreadyOpen raised when trying to open a database that is already open
	errDatabaseAlreadyOpen = errors.New("database is already open")

	// errDatabaseAlreadyClosed raised when trying to open a database that is already closed
	errDatabaseAlreadyClosed = errors.New("database is already closed")

	// errDatabaseNotBootstrapped raised when trying to query a database that's not yet bootstrapped
	errDatabaseNotBootstrapped = errors.New("database not yet bootstrapped")
)

const (
	dbOngoingTasks = 1
)

// Database is a time series database
type Database interface {

	// GetOptions returns the database options
	GetOptions() memtsdb.DatabaseOptions

	// Open will open the database for writing and reading
	Open() error

	// Close will close the database for writing and reading
	Close() error

	// Write value to the database for an ID
	Write(
		id string,
		timestamp time.Time,
		value float64,
		unit time.Duration,
		annotation []byte,
	) error

	// Fetch retrieves encoded segments for an ID
	FetchEncodedSegments(id string, start, end time.Time) (io.Reader, error)

	// Bootstrap bootstraps the database.
	Bootstrap(writeStart time.Time) error
}

type db struct {
	sync.RWMutex
	opts        memtsdb.DatabaseOptions
	shardScheme sharding.ShardScheme
	shardSet    sharding.ShardSet
	bs          bootstrapState

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	nowFn        memtsdb.NowFn
	tickDeadline time.Duration
	openCh       chan struct{}
	doneCh       chan struct{}
}

// NewDatabase creates a new database
func NewDatabase(shardSet sharding.ShardSet, opts memtsdb.DatabaseOptions) Database {
	shardScheme := shardSet.Scheme()
	return &db{
		opts:         opts,
		shardScheme:  shardScheme,
		shardSet:     shardSet,
		shards:       make([]databaseShard, len(shardScheme.All().Shards())),
		nowFn:        opts.GetNowFn(),
		tickDeadline: opts.GetBufferFlush(),
		openCh:       make(chan struct{}, 1),
		doneCh:       make(chan struct{}, dbOngoingTasks),
	}
}

func (d *db) GetOptions() memtsdb.DatabaseOptions {
	return d.opts
}

func (d *db) Open() error {
	select {
	case d.openCh <- struct{}{}:
	default:
		return errDatabaseAlreadyOpen
	}
	d.Lock()
	defer d.Unlock()
	// Initialize shards
	for _, x := range d.shardSet.Shards() {
		d.shards[x] = newDatabaseShard(x, d.opts)
	}
	// All goroutines must be accounted for with dbOngoingTasks to receive done signal
	go d.ongoingTick()
	return nil
}

func (d *db) getOwnedShards() []databaseShard {
	d.RLock()
	shards := d.shardSet.Shards()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = d.shards[shard]
	}
	d.RUnlock()
	return databaseShards
}

// Bootstrap performs bootstrapping for all shards owned by db, assuming the servers
// start accepting writes at writeStart. It returns an error if the server is currently
// being bootstrapped, and nil otherwise.
func (d *db) Bootstrap(writeStart time.Time) error {
	if success, err := tryBootstrap(&d.RWMutex, &d.bs, "database"); !success {
		return err
	}

	d.Lock()
	d.bs = bootstrapping
	d.Unlock()

	shards := d.getOwnedShards()

	// NB(xichen): each bootstrapper should be responsible for choosing the most
	// efficient way of bootstrapping database shards, be it sequential or parallel.
	// In particular, the filesystem bootstrapper bootstraps each shard sequentially
	// due to disk seek overhead.
	for _, s := range shards {
		if err := s.bootstrap(writeStart); err != nil {
			return err
		}
	}

	d.Lock()
	d.bs = bootstrapped
	d.Unlock()

	return nil
}

func (d *db) Close() error {
	select {
	case _ = <-d.openCh:
	default:
		return errDatabaseAlreadyClosed
	}
	d.Lock()
	defer d.Unlock()
	// For now just remove all shards, in future this could be made more explicit.  However
	// this is nice as we do not need to do any other branching now in write/read methods.
	for i := range d.shards {
		d.shards[i] = nil
	}
	for i := 0; i < dbOngoingTasks; i++ {
		d.doneCh <- struct{}{}
	}
	return nil
}

func (d *db) Write(
	id string,
	timestamp time.Time,
	value float64,
	unit time.Duration,
	annotation []byte,
) error {
	d.RLock()
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID]
	d.RUnlock()
	if shard == nil {
		return fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard.write(id, timestamp, value, unit, annotation)
}

func (d *db) FetchEncodedSegments(id string, start, end time.Time) (io.Reader, error) {
	d.RLock()
	if d.bs != bootstrapped {
		d.RUnlock()
		return nil, errDatabaseNotBootstrapped
	}
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID]
	d.RUnlock()
	if shard == nil {
		return nil, fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard.fetchEncodedSegments(id, start, end)
}

func (d *db) ongoingTick() {
	for {
		select {
		case _ = <-d.doneCh:
			return
		default:
			d.splayedTick()
		}
	}
}

func (d *db) splayedTick() {
	shards := d.getOwnedShards()

	splayApart := d.tickDeadline / time.Duration(len(shards))

	start := d.nowFn()

	var wg sync.WaitGroup
	for i, shard := range shards {
		i := i
		shard := shard
		splayWaitDuration := time.Duration(i) * splayApart
		wg.Add(1)
		go func() {
			if splayWaitDuration > 0 {
				<-time.After(splayWaitDuration)
			}
			// TODO(r): instrument timing of this tick
			shard.tick()
			wg.Done()
		}()
	}

	wg.Wait()

	end := d.nowFn()
	duration := end.Sub(start)
	// TODO(r): instrument duration of tick
	if duration > d.tickDeadline {
		// TODO(r): log an error and/or increment counter
		_ = "todo"
	}
}
