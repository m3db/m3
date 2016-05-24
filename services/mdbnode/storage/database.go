package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb/services/mdbnode/sharding"
)

var (
	// ErrDatabaseAlreadyOpen raised when trying to open a database that is already open
	ErrDatabaseAlreadyOpen = errors.New("database is already open")

	// ErrDatabaseAlreadyClosed raised when trying to open a database that is already closed
	ErrDatabaseAlreadyClosed = errors.New("database is already closed")
)

const (
	dbOngoingTasks = 1
)

type db struct {
	sync.RWMutex
	opts        DatabaseOptions
	shardScheme sharding.ShardScheme
	shardSet    sharding.ShardSet

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	nowFn        NowFn
	tickDeadline time.Duration
	openCh       chan struct{}
	doneCh       chan struct{}
}

// NewDatabase creates a new database
func NewDatabase(shardSet sharding.ShardSet, opts DatabaseOptions) Database {
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

func (d *db) GetOptions() DatabaseOptions {
	return d.opts
}

func (d *db) Open() error {
	select {
	case d.openCh <- struct{}{}:
	default:
		return ErrDatabaseAlreadyOpen
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

func (d *db) Close() error {
	select {
	case _ = <-d.openCh:
	default:
		return ErrDatabaseAlreadyClosed
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

func (d *db) FetchEncodedSegments(id string, start, end time.Time) ([][]byte, error) {
	d.RLock()
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
	var shards []databaseShard
	d.RLock()
	for _, shard := range d.shards {
		if shard != nil {
			shards = append(shards, shard)
		}
	}
	d.RUnlock()

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
