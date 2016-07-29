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
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/time"
)

var (
	// errDatabaseAlreadyOpen raised when trying to open a database that is already open
	errDatabaseAlreadyOpen = errors.New("database is already open")

	// errDatabaseAlreadyClosed raised when trying to open a database that is already closed
	errDatabaseAlreadyClosed = errors.New("database is already closed")
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
}

// database is the internal database interface.
type database interface {
	Database

	// getOwnedShards returns the shards this database owns.
	getOwnedShards() []databaseShard

	// flush flushes in-memory data given a start time.
	flush(t time.Time, async bool)
}

type shardAssignment struct {
	shard       databaseShard
	curState    m3db.ShardState
	targetState m3db.ShardState
}

type db struct {
	sync.RWMutex
	opts        m3db.DatabaseOptions
	host        m3db.Host
	topology    m3db.Topology
	shardScheme m3db.ShardScheme
	bsm         databaseBootstrapManager
	fm          databaseFlushManager

	// Contains an entry to all shards for fast shard lookup.
	// If the current database does not own a shard, the
	// databaseShard field in the corresponding entry will be nil.
	shards []shardAssignment

	nowFn        m3db.NowFn
	tickDeadline time.Duration
	openCh       chan struct{}
	doneCh       chan struct{}
}

// NewDatabase creates a new database
func NewDatabase(opts m3db.DatabaseOptions) (Database, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	topologyType := opts.GetTopologyType()
	topology, err := topologyType.Create()
	if err != nil {
		return nil, err
	}
	shardScheme := topologyType.Options().GetShardScheme()
	shards := make([]shardAssignment, len(shardScheme.All().Shards()))

	database := &db{
		opts:         opts,
		host:         opts.GetLocalHost(),
		topology:     topology,
		shardScheme:  shardScheme,
		shards:       shards,
		nowFn:        opts.GetNowFn(),
		tickDeadline: opts.GetBufferDrain(),
		openCh:       make(chan struct{}, 1),
		doneCh:       make(chan struct{}, dbOngoingTasks),
	}
	database.bsm = newBootstrapManager(database)
	database.fm = newFlushManager(database)
	return database, nil
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

	subscriber := make(chan m3db.TopologyMap, 1)
	curTopologyMap := d.topology.GetAndSubscribe(subscriber)
	d.setTopologyMap(curTopologyMap)

	// TODO(xichen): account for this in dbOngoingTasks.
	go func() {
		for value := range subscriber {
			d.setTopologyMap(value)
		}
	}()

	// NB(xichen): use a different goroutine to scan shard assignments to
	// avoid blocking the goroutine that receives config updates during bootstrapping,
	// and at the same time avoid bootstrapping multiple shards simultaneously.
	// Can probably achieve the same effect using a bootstrapLock and spin
	// up bootstrap goroutines as needed but feels cleaner this way.
	go d.processShardAssignments()

	// All goroutines must be accounted for with dbOngoingTasks to receive done signal
	go d.ongoingTick()

	return nil
}

func (d *db) setTopologyMap(tm m3db.TopologyMap) {
	newAssignments := tm.ShardAssignments().GetAssignmentsFor(d.host)
	targetShardAssigments := make(map[uint32]m3db.ShardState)
	for _, assignment := range newAssignments {
		targetShardAssigments[assignment.ShardID()] = assignment.ShardState()
	}
	d.Lock()
	for i := range d.shards {
		targetShardState, exists := targetShardAssigments[uint32(i)]
		if !exists {
			d.shards[i].targetState = m3db.ShardUnassigned
		} else {
			d.shards[i].targetState = targetShardState
		}
	}
	d.Unlock()
}

// processShardAssignments periodically checks shard assignments and takes necessary actions.
func (d *db) processShardAssignments() {
	shardWithChanges := make([]uint32, 0, len(d.shardScheme.All().Shards()))
	period := d.opts.GetShardAssignmentProcessingPeriod()
	for {
		start := d.nowFn()
		shardWithChanges = shardWithChanges[:0]
		d.RLock()
		for shardID := range d.shards {
			if d.shards[shardID].curState != d.shards[shardID].targetState {
				shardWithChanges = append(shardWithChanges, uint32(shardID))
			}
		}
		d.RUnlock()
		for _, shardID := range shardWithChanges {
			d.processAssignmentChangeFor(shardID)
		}
		end := d.nowFn()
		if d := end.Sub(start); d < period {
			time.Sleep(period - d)
		}
	}
}

func (d *db) createShardAssignmentUpdate() m3db.TopologyUpdate {
	var assignments []m3db.ShardAssignment
	d.RLock()
	for id, shard := range d.shards {
		if shard.curState != m3db.ShardUnassigned {
			assignments = append(assignments, topology.NewShardAssignment(uint32(id), shard.curState))
		}
	}
	d.RUnlock()
	return topology.NewShardAssignmentUpdate(d.host, assignments)
}

func (d *db) processAssignmentChangeFor(shard uint32) error {
	log := d.opts.GetLogger()

	d.Lock()
	sa := d.shards[shard]
	if sa.curState == sa.targetState {
		d.Unlock()
		return nil
	}
	d.shards[shard].curState = d.shards[shard].targetState

	// We received a new shard so we need to initialize and bootstrap it.
	if sa.curState == m3db.ShardUnassigned && sa.targetState == m3db.ShardInitializing {
		if d.shards[shard].shard == nil {
			d.shards[shard].shard = newDatabaseShard(shard, d.opts)
		}
		d.Unlock()
		// NB(xichen): assume bootstrapping finished regardless of errors.
		if err := d.bsm.Bootstrap(sa.shard); err != nil {
			log.Errorf("error encountered during bootstrapping shard %d: %v", shard, err)
		}
		// Update the external source about the shard state change.
		update := d.createShardAssignmentUpdate()
		d.topology.PostUpdate(update)
		return nil
	}

	// The shard has finished initialization and been marked as available, nothing to do
	// here other than updating the current shard state.
	if sa.curState == m3db.ShardInitializing && sa.targetState == m3db.ShardAvailable {
		d.Unlock()
		return nil
	}

	// The shard is no longer owned by us, so remove it.
	if sa.curState == m3db.ShardAvailable && sa.targetState == m3db.ShardUnassigned {
		d.shards[shard].shard = nil
		d.Unlock()
		return nil
	}

	d.Unlock()
	return fmt.Errorf("unexpected shard assignment state change from %v to %v", sa.curState, sa.targetState)
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
		d.shards[i] = shardAssignment{}
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
	shard := d.shards[shardID].shard
	shardState := d.shards[shardID].curState
	d.RUnlock()
	if shardState == m3db.ShardUnassigned {
		return fmt.Errorf("not responsible for shard %d", shardID)
	}
	// NB(xichen): if the shard is initializing or available, we allow the writes to go through.
	return shard.Write(ctx, id, timestamp, value, unit, annotation)
}

func (d *db) ReadEncoded(
	ctx m3db.Context,
	id string,
	start, end time.Time,
) ([][]m3db.SegmentReader, error) {
	d.RLock()
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID].shard
	shardState := d.shards[shardID].curState
	d.RUnlock()
	if shardState == m3db.ShardUnassigned {
		return nil, fmt.Errorf("not responsible for shard %d", shardID)
	}
	if shardState == m3db.ShardInitializing {
		return nil, fmt.Errorf("shard %d is still initializing", shardID)
	}
	return shard.ReadEncoded(ctx, id, start, end)
}

func (d *db) getOwnedShards() []databaseShard {
	var ownedShards []databaseShard
	d.RLock()
	for _, shard := range d.shards {
		if shard.curState == m3db.ShardAvailable {
			ownedShards = append(ownedShards, shard.shard)
		}
	}
	d.RUnlock()
	return ownedShards
}

func (d *db) flush(t time.Time, async bool) {
	d.fm.Flush(t, async)
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

	if d.fm.NeedsFlush(start) {
		d.fm.Flush(start, true)
	}

	end := d.nowFn()
	duration := end.Sub(start)
	// TODO(r): instrument duration of tick
	if duration > d.tickDeadline {
		// TODO(r): log an error and/or increment counter
		_ = "todo"
	} else {
		// throttle to reduce locking overhead during ticking
		time.Sleep(d.tickDeadline - duration)
	}
}
