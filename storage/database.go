// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
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

	// Options returns the database options
	Options() m3db.DatabaseOptions

	// Open will open the database for writing and reading
	Open() error

	// Close will close the database for writing and reading
	Close() error

	// Write value to the database for an ID
	Write(
		ctx m3db.Context,
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded retrieves encoded segments for an ID
	ReadEncoded(
		ctx m3db.Context,
		id string,
		start, end time.Time,
	) ([][]m3db.SegmentReader, error)

	// Bootstrap bootstraps the database.
	Bootstrap(writeStart time.Time) error
}

type flushStatus byte

const (
	flushNotStarted flushStatus = iota
	flushInProgress
	flushSuccess
	flushFailed
)

type flushState struct {
	status      flushStatus
	numFailures int
}

type db struct {
	sync.RWMutex
	opts           m3db.DatabaseOptions
	shardScheme    m3db.ShardScheme
	shardSet       m3db.ShardSet
	bs             bootstrapState
	fm             sync.RWMutex
	fs             flushStatus
	flushAttempted map[time.Time]flushState
	flushTimes     []time.Time

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	nowFn        m3db.NowFn
	tickDeadline time.Duration
	openCh       chan struct{}
	doneCh       chan struct{}
}

// NewDatabase creates a new database
func NewDatabase(shardSet m3db.ShardSet, opts m3db.DatabaseOptions) Database {
	shardScheme := shardSet.Scheme()
	return &db{
		opts:           opts,
		shardScheme:    shardScheme,
		shardSet:       shardSet,
		shards:         make([]databaseShard, len(shardScheme.All().Shards())),
		nowFn:          opts.GetNowFn(),
		tickDeadline:   opts.GetBufferDrain(),
		openCh:         make(chan struct{}, 1),
		doneCh:         make(chan struct{}, dbOngoingTasks),
		fs:             flushNotStarted,
		flushAttempted: make(map[time.Time]flushState),
		flushTimes:     make([]time.Time, 0, int(math.Ceil(float64(opts.GetRetentionPeriod())/float64(opts.GetBlockSize())))),
	}
}

func (d *db) Options() m3db.DatabaseOptions {
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
		if err := s.Bootstrap(writeStart); err != nil {
			return err
		}
	}

	// NB(xichen): when we get here, we should have bootstrapped everything between
	// writeStart - retentionPeriod and writeStart + bufferFuture, which means the
	// current time is at least writeStart + bufferFuture + bufferPast. We intentionally
	// don't use now because we don't know whether the in-memory buffers have been drained.
	flushTime := writeStart.Add(d.opts.GetBufferFuture()).Add(d.opts.GetBufferPast())
	d.flushToDisk(flushTime, false)

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
	ctx m3db.Context,
	id string,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	d.RLock()
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID]
	d.RUnlock()
	if shard == nil {
		return fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard.Write(ctx, id, timestamp, value, unit, annotation)
}

func (d *db) ReadEncoded(
	ctx m3db.Context,
	id string,
	start, end time.Time,
) ([][]m3db.SegmentReader, error) {
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
	return shard.ReadEncoded(ctx, id, start, end)
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
		if i > 0 {
			time.Sleep(splayApart)
		}
		wg.Add(1)
		go func() {
			// TODO(r): instrument timing of this tick
			shard.Tick()
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

	if d.needDiskFlush(start) {
		d.flushToDisk(start, true)
	}
}

func (d *db) needDiskFlush(tickStart time.Time) bool {
	d.RLock()
	bs := d.bs
	d.RUnlock()
	// If we haven't bootstrapped yet, don't flush.
	if bs != bootstrapped {
		return false
	}

	firstBlockStart := d.getFirstBlockStart(tickStart)
	d.fm.RLock()
	defer d.fm.RUnlock()
	// If we are in the middle of flushing data, don't flush.
	if d.fs == flushInProgress {
		return false
	}
	// If we have already tried flushing for this block start time, don't try again.
	if _, exists := d.flushAttempted[firstBlockStart]; exists {
		return false
	}
	return true
}

func (d *db) getFirstBlockStart(tickStart time.Time) time.Time {
	bufferPast := d.opts.GetBufferPast()
	blockSize := d.opts.GetBlockSize()
	return tickStart.Add(-bufferPast).Add(-blockSize).Truncate(blockSize)
}

func (d *db) flushToDisk(tickStart time.Time, asyncFlush bool) {
	timesToFlush := d.getTimesToFlush(tickStart)
	if len(timesToFlush) == 0 {
		return
	}

	d.fm.Lock()
	if d.fs == flushInProgress {
		d.fm.Unlock()
		return
	}
	d.fs = flushInProgress
	d.fm.Unlock()

	flushFn := func() {
		for _, t := range timesToFlush {
			success := d.flushToDiskWithTime(t)
			d.fm.Lock()
			flushState := d.flushAttempted[t]
			if success {
				flushState.status = flushSuccess
			} else {
				flushState.status = flushFailed
				flushState.numFailures++
			}
			d.flushAttempted[t] = flushState
			d.fm.Unlock()
		}

		d.fm.Lock()
		d.fs = flushNotStarted
		d.fm.Unlock()
	}

	if !asyncFlush {
		flushFn()
	} else {
		go flushFn()
	}
}

func (d *db) getTimesToFlush(tickStart time.Time) []time.Time {
	blockSize := d.opts.GetBlockSize()
	maxFlushRetries := d.opts.GetMaxFlushRetries()
	firstBlockStart := d.getFirstBlockStart(tickStart)
	earliestTime := tickStart.Add(-d.opts.GetRetentionPeriod())

	d.fm.Lock()
	defer d.fm.Unlock()
	d.flushTimes = d.flushTimes[:0]
	for t := firstBlockStart; !t.Before(earliestTime); t = t.Add(-blockSize) {
		if flushState, exists := d.flushAttempted[t]; !exists || (flushState.status == flushFailed && flushState.numFailures < maxFlushRetries) {
			flushState.status = flushInProgress
			d.flushTimes = append(d.flushTimes, t)
			d.flushAttempted[t] = flushState
		}
	}
	return d.flushTimes
}

// flushToDiskWithTime flushes data blocks for owned shards to local disk.
func (d *db) flushToDiskWithTime(t time.Time) bool {
	allShardsSucceeded := true
	log := d.opts.GetLogger()
	shards := d.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data to disk.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.FlushToDisk(t); err != nil {
			log.Errorf("shard %d failed to flush data to disk: %v", shard.ShardNum(), err)
			allShardsSucceeded = false
		}
	}
	return allShardsSucceeded
}
