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
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/m3db/m3db/topology"
	"github.com/uber-go/tally"
)

var (
	errNoRepairOptions  = errors.New("no repair options")
	errRepairInProgress = errors.New("repair already in progress")
)

type recordFn func(namespace ts.ID, shard databaseShard, res repair.Result)
type repairShardFn func(namespace ts.ID, shard databaseShard, diffRes repair.MetadataComparisonResult) (repair.ExecutionMetrics, error)

type shardRepairer struct {
	opts      Options
	rpopts    repair.Options
	rtopts    retention.Options
	client    client.AdminClient
	recordFn  recordFn
	repairFn  repairShardFn
	logger    xlog.Logger
	scope     tally.Scope
	nowFn     clock.NowFn
	blockSize time.Duration
}

func newShardRepairer(opts Options, rpopts repair.Options) (databaseShardRepairer, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("database.repair").Tagged(map[string]string{"host": hostname})
	rtopts := opts.RetentionOptions()

	r := shardRepairer{
		opts:      opts,
		rpopts:    rpopts,
		rtopts:    rtopts,
		client:    rpopts.AdminClient(),
		logger:    iopts.Logger(),
		scope:     scope,
		nowFn:     opts.ClockOptions().NowFn(),
		blockSize: rtopts.BlockSize(),
	}
	r.recordFn = r.recordDifferences
	r.repairFn = r.repairDifferences

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
) (repair.Result, error) {
	session, err := r.client.DefaultAdminSession()
	if err != nil {
		return repair.Result{}, err
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
	localMetadata, _ := shard.FetchBlocksMetadata(ctx, start, end, math.MaxInt64, 0, true, true)
	ctx.RegisterFinalizer(context.FinalizerFn(localMetadata.Close))

	localIter := block.NewFilteredBlocksMetadataIter(localMetadata)
	metadata.AddLocalMetadata(origin, localIter)

	// Add peer metadata
	peerIter, err := session.FetchBlocksMetadataFromPeers(namespace, shard.ID(), start, end)
	if err != nil {
		return repair.Result{}, err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return repair.Result{}, err
	}

	metadataRes := metadata.Compare()
	result := repair.Result{DifferenceSummary: metadataRes}

	res, err := r.repairFn(namespace, shard, metadataRes)
	if err == nil {
		result.RepairSummary = res
	}
	r.recordFn(namespace, shard, result)
	return result, err
}

type hostSet map[string]repair.HostBlockMetadata

type blockID struct {
	idHash ts.Hash
	start  time.Time
}

func newHostSet(numReplicas int) hostSet {
	hs := make(hostSet, numReplicas)
	return hs
}

func (h hostSet) insert(hbm repair.HostBlockMetadata) {
	if host := hbm.Host; host != nil {
		h[host.String()] = hbm
	}
}

func (h hostSet) insertOneFromSet(oh hostSet) {
	for _, hbm := range oh {
		h.insert(hbm)
		return
	}
}

func (h hostSet) contains(host topology.Host) bool {
	if host == nil {
		return false
	}
	_, ok := h[host.String()]
	return ok
}

func (h hostSet) remove(host topology.Host) {
	if host != nil {
		delete(h, host.String())
	}
}

func (h hostSet) empty() bool {
	return len(h) == 0
}

func (r shardRepairer) newRequest(
	pendingReplicasMap map[blockID]hostSet,
	idMap map[ts.Hash]ts.ID,
	initialCapacity int64,
) []block.ReplicaMetadata {
	repairBlocks := make([]block.ReplicaMetadata, 0, initialCapacity)
	for blkID, hs := range pendingReplicasMap {
		for _, h := range hs {
			repairBlocks = append(repairBlocks, block.ReplicaMetadata{
				ID:   idMap[blkID.idHash],
				Host: h.Host,
				Metadata: block.Metadata{
					Start:    blkID.start,
					Checksum: h.Checksum,
					Size:     h.Size,
				},
			})
		}
	}
	return repairBlocks
}

func pendingReplicasHelper(
	pendingReplicas map[blockID]hostSet,
	idMap map[ts.Hash]ts.ID,
	originHost topology.Host,
	numReplicas int,
	diffFn func() map[ts.Hash]repair.ReplicaSeriesBlocksMetadata,
	valueFn func(repair.HostBlockMetadata) (int64, bool),
) {
	for _, rsm := range diffFn() {
		idHash := rsm.ID.Hash()
		idMap[idHash] = rsm.ID
		for start, blk := range rsm.Metadata.Blocks() {
			blkID := blockID{idHash, start}
			// find all unique sizes seen for this block
			uniqueValues := make(map[int64]hostSet, numReplicas)
			for _, hBlk := range blk.Metadata() {
				sz, ok := valueFn(hBlk)
				if !ok {
					continue
				}
				if _, ok := uniqueValues[sz]; !ok {
					uniqueValues[sz] = newHostSet(numReplicas)
				}
				uniqueValues[sz].insert(hBlk)
			}
			for _, hs := range uniqueValues {
				if hs.contains(originHost) {
					// we already have originHost data available locally, no need to fetch it
					continue
				}
				// insert value into replicaState
				if _, ok := pendingReplicas[blkID]; !ok {
					pendingReplicas[blkID] = newHostSet(numReplicas)
				}
				// only care about a single value from the unique set
				pendingReplicas[blkID].insertOneFromSet(hs)
			}
		}
	}
}

// newPendingReplicasMap returns a map from (series_id, block_start) -> set of host replicas,
// using which repairs are to be performed
//
// NB(prateek): In the scenario when multiple peers have identical replicas for a single block, the current
// implementation arbitrarily picks one of the peers as the source to request the replica from. In the event
// the replica is unavailable from the selected peer, the current implementation fails. If this becomes an issue,
// the code should enhanced to provide a list of potential sources for a single replica (as opposed to the
// non-deterministic selection)
func (r shardRepairer) newPendingReplicasMap(
	diffRes repair.MetadataComparisonResult,
	originHost topology.Host,
	numReplicas int,
) (map[blockID]hostSet, map[ts.Hash]ts.ID) {
	// TODO(prateek): pooling object creation in this method
	pendingReplicas := make(map[blockID]hostSet, diffRes.NumBlocks)
	idMap := make(map[ts.Hash]ts.ID, diffRes.NumBlocks)

	// add size differences
	pendingReplicasHelper(pendingReplicas, idMap, originHost, numReplicas, diffRes.SizeDifferences.Series,
		func(h repair.HostBlockMetadata) (int64, bool) {
			return h.Size, true
		},
	)

	// add checksum differences
	pendingReplicasHelper(pendingReplicas, idMap, originHost, numReplicas, diffRes.ChecksumDifferences.Series,
		func(h repair.HostBlockMetadata) (int64, bool) {
			cs := h.Checksum
			if cs == nil {
				return 0, false
			}
			return int64(*cs), true
		},
	)

	return pendingReplicas, idMap
}

// TODO(prateek): better handling of repaired shard writes
// - Versioning
//   - write a new version of the file for the timestamp
//   - keep last 'n' versions
//   - do NOT delete old version before writing a new version
// - Smarter triggering of writes
//   - consider not always writing
//   - factor in a minimum number of blocks repaired
//   - and number of blocks still requiring repair
func (r shardRepairer) repairDifferences(
	namespace ts.ID,
	shard databaseShard,
	diffRes repair.MetadataComparisonResult,
) (repair.ExecutionMetrics, error) {
	var (
		logger        = r.opts.InstrumentOptions().Logger()
		session, err  = r.client.DefaultAdminSession()
		repairSummary = repair.ExecutionMetrics{}
		multiErr      xerrors.MultiError
	)
	if err != nil {
		return repairSummary, err
	}
	numReplicas := session.Replicas()

	// pendingBlockReplicas is a map from (series_id, block_start) -> set of host replicas for which repairs are pending
	pendingBlockReplicas, hashToIDMap := r.newPendingReplicasMap(diffRes, session.Origin(), numReplicas)

	// reqBlocks contains the same information as the above map, destructured for the `session` API
	initialCap := diffRes.NumBlocks
	reqBlocks := r.newRequest(pendingBlockReplicas, hashToIDMap, initialCap)
	repairSummary.NumRequested = int64(len(reqBlocks))

	// stream over the requested replicas
	blocksIter, err := session.FetchBlocksFromPeers(namespace, shard.ID(), reqBlocks, r.rpopts.ResultOptions())
	if err != nil {
		return repairSummary, err
	}

	// amortizing the times marked dirty by UpdateSeries
	dirtyBlockTimes := make(map[time.Time]struct{})
	for blocksIter.Next() {
		host, id, blk := blocksIter.Current()

		blkID := blockID{id.Hash(), blk.StartTime()}
		// ensure the replica was requested, should never happen
		if hs, ok := pendingBlockReplicas[blkID]; !ok || !hs.contains(host) {
			logger.WithFields(
				xlog.NewLogField("id", id.String()),
				xlog.NewLogField("host", host.String()),
				xlog.NewLogField("blockID", blkID),
			).Warnf("received unexpected block, session.FetchRepairBlockFromPeers violated contract, skipping.")
			repairSummary.NumUnexpected++
			continue
		}

		// mark the block replica as received
		pendingBlockReplicas[blkID].remove(host)

		if err := shard.UpdateSeries(id, blk); err != nil {
			repErr := fmt.Errorf(
				"unable to update series [ id = %v, block_start = %v, err = %v ]",
				id.String(), blkID.start, err)
			multiErr = multiErr.Add(repErr)
			logger.Warnf("%v", repErr)
			repairSummary.NumFailed++
			continue
		}

		dirtyBlockTimes[blkID.start] = struct{}{}
		repairSummary.NumRepaired++
	}

	// marking flush states dirty
	dirtyBlockTimesList := make([]time.Time, 0, len(dirtyBlockTimes))
	for t := range dirtyBlockTimes {
		dirtyBlockTimesList = append(dirtyBlockTimesList, t)
	}
	shard.MarkFlushStatesDirty(dirtyBlockTimesList)

	// track count for pending replicas
	for blkID, hs := range pendingBlockReplicas {
		if hs.empty() {
			continue
		}
		for _, hbm := range hs {
			missingErr := fmt.Errorf(
				"replica requested but not received [ host = %v, id = %v, block_start = %v ]",
				hbm.Host.String(), hashToIDMap[blkID.idHash].String(), blkID.start)
			multiErr = multiErr.Add(missingErr)
			logger.Warnf("%v", missingErr)
			repairSummary.NumPending++
		}
	}

	// track error if blocksIter is unhappy
	if err := blocksIter.Err(); err != nil {
		multiErr = multiErr.Add(err)
		logger.Warnf("repair iterator terminated with error: %v", err)
	}

	return repairSummary, multiErr.FinalError()
}

func (r shardRepairer) recordDifferences(
	namespace ts.ID,
	shard databaseShard,
	repairResult repair.Result,
) {
	var (
		shardScope = r.scope.Tagged(map[string]string{
			"namespace": namespace.String(),
			"shard":     strconv.Itoa(int(shard.ID())),
		})
		totalScope        = shardScope.Tagged(map[string]string{"resultType": "total"})
		sizeDiffScope     = shardScope.Tagged(map[string]string{"resultType": "sizeDiff"})
		checksumDiffScope = shardScope.Tagged(map[string]string{"resultType": "checksumDiff"})
		executionScope    = shardScope.Tagged(map[string]string{"resultType": "execution"})
		diffRes           = repairResult.DifferenceSummary
		execMetrics       = repairResult.RepairSummary
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

	// Record ExecutionMetrics
	executionScope.Counter("failed").Inc(execMetrics.NumFailed)
	executionScope.Counter("pending").Inc(execMetrics.NumPending)
	executionScope.Counter("repaired").Inc(execMetrics.NumRepaired)
	executionScope.Counter("requested").Inc(execMetrics.NumRequested)
	executionScope.Counter("unexpected").Inc(execMetrics.NumUnexpected)
}

type repairFn func(time.Time) error

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

type dbRepairer struct {
	sync.Mutex

	database      database
	ropts         repair.Options
	rtopts        retention.Options
	shardRepairer databaseShardRepairer
	repairStates  map[time.Time]repairState

	repairFn         repairFn
	nowFn            clock.NowFn
	logger           xlog.Logger
	repairInterval   time.Duration
	repairTimeJitter time.Duration
	repairMaxRetries int
	running          int32
	lastRun          time.Time
}

func newDatabaseRepairer(database database) (databaseRepairer, error) {
	opts := database.Options()
	nowFn := opts.ClockOptions().NowFn()
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
		database:         database,
		ropts:            ropts,
		rtopts:           opts.RetentionOptions(),
		shardRepairer:    shardRepairer,
		repairStates:     make(map[time.Time]repairState),
		nowFn:            nowFn,
		logger:           opts.InstrumentOptions().Logger(),
		repairInterval:   ropts.RepairInterval(),
		repairTimeJitter: jitter,
		repairMaxRetries: ropts.RepairMaxRetries(),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) ShouldRun(start time.Time) bool {
	if r.IsRepairing() {
		return false
	}
	maxDelta := r.repairInterval + r.repairTimeJitter
	return start.Sub(r.lastRun) > maxDelta
}

func (r *dbRepairer) Run(start time.Time, mode runType) {
	run := func() {
		if err := r.repairFn(start); err != nil {
			r.logger.Errorf("error repairing database: %v", err)
		}
		// setting lastRun regardless of success/failure
		// to constrain the number of repairs run
		r.lastRun = start
	}

	if mode == runTypeAsync {
		go run()
	} else {
		run()
	}
}

func (r *dbRepairer) repairTimeRanges(startTime time.Time) xtime.Ranges {
	var (
		blockSize = r.rtopts.BlockSize()
		// only attempt repairs on data that has been flushed
		start = retention.FlushTimeStart(r.rtopts, startTime)
		end   = retention.FlushTimeEnd(r.rtopts, startTime).Add(-blockSize)
	)

	targetRanges := xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end})
	for t := range r.repairStates {
		if !r.needsRepair(t) {
			targetRanges = targetRanges.RemoveRange(xtime.Range{Start: t, End: t.Add(blockSize)})
		}
	}

	return targetRanges
}

func (r *dbRepairer) needsRepair(t time.Time) bool {
	repairState, exists := r.repairStates[t]
	if !exists {
		return true
	}
	return repairState.Status == repairFailed && repairState.NumFailures < r.repairMaxRetries
}

func (r *dbRepairer) Repair(startTime time.Time) error {
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
	blockSize := r.rtopts.BlockSize()
	iter := r.repairTimeRanges(startTime).Iter()
	for iter.Next() {
		tr := iter.Value()
		err := r.repairWithTimeRange(tr)
		for t := tr.Start; t.Before(tr.End); t = t.Add(blockSize) {
			repairState := r.repairStates[t]
			if err == nil {
				repairState.Status = repairSuccess
			} else {
				repairState.Status = repairFailed
				repairState.NumFailures++
			}
			r.repairStates[t] = repairState
		}
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (r *dbRepairer) repairWithTimeRange(tr xtime.Range) error {
	multiErr := xerrors.NewMultiError()
	namespaces := r.database.getOwnedNamespaces()
	for _, n := range namespaces {
		if err := n.Repair(r.shardRepairer, tr); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to repair time range %v: %v", n.ID().String(), tr, err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

func (r *dbRepairer) IsRepairing() bool {
	return atomic.LoadInt32(&r.running) == 1
}

type noopRepairer struct{}

func newNoopDatabaseRepairer() databaseRepairer {
	return noopRepairer{}
}

func (r noopRepairer) ShouldRun(start time.Time) bool    { return false }
func (r noopRepairer) Run(start time.Time, mode runType) {}
func (r noopRepairer) IsRepairing() bool                 { return false }
