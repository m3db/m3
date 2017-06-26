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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errNoRepairOptions  = errors.New("no repair options")
	errRepairInProgress = errors.New("repair already in progress")
)

type recordFn func(namespace ts.ID, shard databaseShard, diffRes repair.MetadataComparisonResult)

type shardRepairer struct {
	opts     Options
	rpopts   repair.Options
	client   client.AdminClient
	recordFn recordFn
	logger   xlog.Logger
	scope    tally.Scope
	nowFn    clock.NowFn
}

func newShardRepairer(opts Options, rpopts repair.Options) (databaseShardRepairer, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("database.repair").Tagged(map[string]string{"host": hostname})

	r := shardRepairer{
		opts:   opts,
		rpopts: rpopts,
		client: rpopts.AdminClient(),
		logger: iopts.Logger(),
		scope:  scope,
		nowFn:  opts.ClockOptions().NowFn(),
	}
	r.recordFn = r.recordDifferences

	return r, nil
}

func (r shardRepairer) Options() repair.Options {
	return r.rpopts
}

func (r shardRepairer) Repair(
	ctx context.Context,
	namespace ts.ID,
	tr xtime.Range,
	shard databaseShard,
) (repair.MetadataComparisonResult, error) {
	session, err := r.client.DefaultAdminSession()
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	var (
		start    = tr.Start
		end      = tr.End
		origin   = session.Origin()
		replicas = session.Replicas()
	)

	metadata := repair.NewReplicaMetadataComparer(replicas, r.rpopts)
	ctx.RegisterFinalizer(metadata)

	// Add local metadata
	opts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     true,
		IncludeChecksums: true,
	}
	localMetadata, _ := shard.FetchBlocksMetadata(ctx, start, end, math.MaxInt64, 0, opts)
	ctx.RegisterFinalizer(context.FinalizerFn(localMetadata.Close))

	localIter := block.NewFilteredBlocksMetadataIter(localMetadata)
	metadata.AddLocalMetadata(origin, localIter)

	// Add peer metadata
	peerIter, err := session.FetchBlocksMetadataFromPeers(namespace, shard.ID(), start, end)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	metadataRes := metadata.Compare()

	r.recordFn(namespace, shard, metadataRes)

	return metadataRes, nil
}

func (r shardRepairer) recordDifferences(
	namespace ts.ID,
	shard databaseShard,
	diffRes repair.MetadataComparisonResult,
) {
	var (
		shardScope = r.scope.Tagged(map[string]string{
			"namespace": namespace.String(),
			"shard":     strconv.Itoa(int(shard.ID())),
		})
		totalScope        = shardScope.Tagged(map[string]string{"resultType": "total"})
		sizeDiffScope     = shardScope.Tagged(map[string]string{"resultType": "sizeDiff"})
		checksumDiffScope = shardScope.Tagged(map[string]string{"resultType": "checksumDiff"})
	)

	// Record total number of series and total number of blocks
	totalScope.Counter("series").Inc(diffRes.NumSeries)
	totalScope.Counter("blocks").Inc(diffRes.NumBlocks)

	// Record size differences
	sizeDiffScope.Counter("series").Inc(diffRes.SizeDifferences.NumSeries())
	sizeDiffScope.Counter("blocks").Inc(diffRes.SizeDifferences.NumBlocks())

	// Record checksum differences
	checksumDiffScope.Counter("series").Inc(diffRes.ChecksumDifferences.NumSeries())
	checksumDiffScope.Counter("blocks").Inc(diffRes.ChecksumDifferences.NumBlocks())
}

type repairFn func() error

type sleepFn func(d time.Duration)

type repairStatus int

const (
	repairNotStarted repairStatus = iota
	repairSuccess
	repairFailed
)

type repairState struct {
	Status      repairStatus
	NumFailures int
}

type namespaceRepairStateByTime map[time.Time]repairState

type repairStatesByTime map[ts.Hash]namespaceRepairStateByTime

func newRepairStates() repairStatesByTime {
	return make(repairStatesByTime)
}

func (r repairStatesByTime) get(
	namespace ts.ID,
	t time.Time,
) (repairState, bool) {
	var rs repairState

	nsRepairState, ok := r[namespace.Hash()]
	if !ok {
		return rs, false
	}

	rs, ok = nsRepairState[t]
	return rs, ok
}

func (r repairStatesByTime) set(
	namespace ts.ID,
	t time.Time,
	state repairState,
) {
	nsRepairState, ok := r[namespace.Hash()]
	if !ok {
		nsRepairState = make(namespaceRepairStateByTime)
	}
	nsRepairState[t] = state
	r[namespace.Hash()] = nsRepairState
}

type dbRepairer struct {
	sync.Mutex

	database      database
	ropts         repair.Options
	shardRepairer databaseShardRepairer
	repairStates  repairStatesByTime

	repairFn            repairFn
	sleepFn             sleepFn
	nowFn               clock.NowFn
	logger              xlog.Logger
	repairInterval      time.Duration
	repairTimeOffset    time.Duration
	repairTimeJitter    time.Duration
	repairCheckInterval time.Duration
	repairMaxRetries    int
	closed              bool
	running             int32
	status              tally.Gauge
}

func newDatabaseRepairer(database database, opts Options) (databaseRepairer, error) {
	nowFn := opts.ClockOptions().NowFn()
	scope := opts.InstrumentOptions().MetricsScope()
	ropts := opts.RepairOptions()
	if ropts == nil {
		return nil, errNoRepairOptions
	}
	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	shardRepairer, err := newShardRepairer(opts, ropts)
	if err != nil {
		return nil, err
	}

	var jitter time.Duration
	if repairJitter := ropts.RepairTimeJitter(); repairJitter > 0 {
		src := rand.NewSource(nowFn().UnixNano())
		jitter = time.Duration(float64(repairJitter) * (float64(src.Int63()) / float64(math.MaxInt64)))
	}

	r := &dbRepairer{
		database:            database,
		ropts:               ropts,
		shardRepairer:       shardRepairer,
		repairStates:        newRepairStates(),
		sleepFn:             time.Sleep,
		nowFn:               nowFn,
		logger:              opts.InstrumentOptions().Logger(),
		repairInterval:      ropts.RepairInterval(),
		repairTimeOffset:    ropts.RepairTimeOffset(),
		repairTimeJitter:    jitter,
		repairCheckInterval: ropts.RepairCheckInterval(),
		repairMaxRetries:    ropts.RepairMaxRetries(),
		status:              scope.Gauge("repair"),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) run() {
	var curIntervalStart time.Time

	for {
		r.Lock()
		closed := r.closed
		r.Unlock()

		if closed {
			break
		}

		r.sleepFn(r.repairCheckInterval)

		now := r.nowFn()
		intervalStart := now.Truncate(r.repairInterval)

		// If we haven't reached the offset yet, skip
		target := intervalStart.Add(r.repairTimeOffset + r.repairTimeJitter)
		if now.Before(target) {
			continue
		}

		// If we are in the same interval, we must have already repaired, skip
		if intervalStart == curIntervalStart {
			continue
		}

		curIntervalStart = intervalStart
		if err := r.repairFn(); err != nil {
			r.logger.Errorf("error repairing database: %v", err)
		}
	}
}

func (r *dbRepairer) namespaceRepairTimeRanges(ns databaseNamespace) xtime.Ranges {
	var (
		now       = r.nowFn()
		rtopts    = ns.Options().RetentionOptions()
		blockSize = rtopts.BlockSize()
		start     = now.Add(-rtopts.RetentionPeriod()).Truncate(blockSize)
		end       = now.Add(-rtopts.BufferPast()).Truncate(blockSize)
	)

	targetRanges := xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end})
	for t := range r.repairStates[ns.ID().Hash()] {
		if !r.needsRepair(ns.ID(), t) {
			targetRanges = targetRanges.RemoveRange(xtime.Range{Start: t, End: t.Add(blockSize)})
		}
	}

	return targetRanges
}

func (r *dbRepairer) needsRepair(ns ts.ID, t time.Time) bool {
	repairState, exists := r.repairStates.get(ns, t)
	if !exists {
		return true
	}
	return repairState.Status == repairNotStarted ||
		(repairState.Status == repairFailed && repairState.NumFailures < r.repairMaxRetries)
}

func (r *dbRepairer) setRepairState(ns ts.ID, t time.Time) bool {
	repairState, exists := r.repairStates.get(ns, t)
	if !exists {
		return true
	}
	return repairState.Status == repairNotStarted ||
		(repairState.Status == repairFailed && repairState.NumFailures < r.repairMaxRetries)
}

func (r *dbRepairer) Start() {
	if r.repairInterval <= 0 {
		return
	}

	go r.run()
}

func (r *dbRepairer) Stop() {
	r.Lock()
	r.closed = true
	r.Unlock()
}

func (r *dbRepairer) Repair() error {
	// Don't attempt a repair if the database is not bootstrapped yet
	if !r.database.IsBootstrapped() {
		return nil
	}

	if !atomic.CompareAndSwapInt32(&r.running, 0, 1) {
		return errRepairInProgress
	}

	defer func() {
		atomic.StoreInt32(&r.running, 0)
	}()

	multiErr := xerrors.NewMultiError()
	for _, n := range r.database.getOwnedNamespaces() {
		iter := r.namespaceRepairTimeRanges(n).Iter()
		for iter.Next() {
			multiErr = multiErr.Add(r.repairNamespaceWithTimeRange(n, iter.Value()))
		}
	}
	return multiErr.FinalError()
}

func (r *dbRepairer) Report() {
	if atomic.LoadInt32(&r.running) == 1 {
		r.status.Update(1)
	} else {
		r.status.Update(0)
	}
}

func (r *dbRepairer) repairNamespaceWithTimeRange(n databaseNamespace, tr xtime.Range) error {
	var (
		rtopts    = n.Options().RetentionOptions()
		blockSize = rtopts.BlockSize()
		err       error
	)

	// repair the namespace
	if err = n.Repair(r.shardRepairer, tr); err != nil {
		err = fmt.Errorf("namespace %s failed to repair time range %v: %v", n.ID().String(), tr, err)
	}

	// update repairer state
	for t := tr.Start; t.Before(tr.End); t = t.Add(blockSize) {
		repairState, _ := r.repairStates.get(n.ID(), t)
		if err == nil {
			repairState.Status = repairSuccess
		} else {
			repairState.Status = repairFailed
			repairState.NumFailures++
		}
		r.repairStates.set(n.ID(), t, repairState)
	}

	return err
}

var noOpRepairer databaseRepairer = repairerNoOp{}

type repairerNoOp struct{}

func newNoopDatabaseRepairer() databaseRepairer { return noOpRepairer }

func (r repairerNoOp) Start()        {}
func (r repairerNoOp) Stop()         {}
func (r repairerNoOp) Repair() error { return nil }
func (r repairerNoOp) Report()       {}
