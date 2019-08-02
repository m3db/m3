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
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/dice"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errNoRepairOptions  = errors.New("no repair options")
	errRepairInProgress = errors.New("repair already in progress")
)

type recordFn func(namespace ident.ID, shard databaseShard, diffRes repair.MetadataComparisonResult)

// TODO(rartoul): See if we can find a way to guard against too much metadata.
type shardRepairer struct {
	opts     Options
	rpopts   repair.Options
	client   client.AdminClient
	recordFn recordFn
	logger   *zap.Logger
	scope    tally.Scope
	nowFn    clock.NowFn
}

func newShardRepairer(opts Options, rpopts repair.Options) databaseShardRepairer {
	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("repair")

	r := shardRepairer{
		opts:   opts,
		rpopts: rpopts,
		client: rpopts.AdminClient(),
		logger: iopts.Logger(),
		scope:  scope,
		nowFn:  opts.ClockOptions().NowFn(),
	}
	r.recordFn = r.recordDifferences

	return r
}

func (r shardRepairer) Options() repair.Options {
	return r.rpopts
}

func (r shardRepairer) Repair(
	ctx context.Context,
	nsCtx namespace.Context,
	nsMeta namespace.Metadata,
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
	var (
		accumLocalMetadata = block.NewFetchBlocksMetadataResults()
		pageToken          PageToken
	)
	// Safe to register since by the time this function completes we won't be using the metadata
	// for anything anymore.
	ctx.RegisterCloser(accumLocalMetadata)

	for {
		// It's possible for FetchBlocksMetadataV2 to not return all the metadata at once even if
		// math.MaxInt64 is passed as the limit due to its implementation and the different phases
		// of the page token. As a result, the only way to ensure that all the metadata has been
		// fetched is to continue looping until a nil pageToken is returned.
		var currLocalMetadata block.FetchBlocksMetadataResults
		currLocalMetadata, pageToken, err = shard.FetchBlocksMetadataV2(ctx, start, end, math.MaxInt64, pageToken, opts)
		if err != nil {
			return repair.MetadataComparisonResult{}, err
		}

		// Merge.
		if currLocalMetadata != nil {
			for _, result := range currLocalMetadata.Results() {
				accumLocalMetadata.Add(result)
			}
		}

		if pageToken == nil {
			break
		}
	}

	if r.rpopts.DebugShadowComparisonsEnabled() {
		// Shadow comparison is mostly a debug feature that can be used to test new builds and diagnose
		// issues with the repair feature. It should not be enabled for production use-cases.
		err := r.shadowCompare(start, end, accumLocalMetadata, session, shard, nsCtx)
		if err != nil {
			r.logger.Error(
				"Shadow compare failed",
				zap.Error(err))
		}
	}

	localIter := block.NewFilteredBlocksMetadataIter(accumLocalMetadata)
	err = metadata.AddLocalMetadata(origin, localIter)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	rsOpts := r.opts.RepairOptions().ResultOptions()
	// Add peer metadata.
	level := r.rpopts.RepairConsistencyLevel()
	peerIter, err := session.FetchBlocksMetadataFromPeers(nsCtx.ID, shard.ID(), start, end,
		level, rsOpts)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	// TODO(rartoul): Pool this slice.
	metadatas := []block.ReplicaMetadata{}
	metadataRes := metadata.Compare()
	for _, e := range metadataRes.ChecksumDifferences.Series().Iter() {
		for blockStart, replicaMetadataBlocks := range e.Value().Metadata.Blocks() {
			blStartTime := blockStart.ToTime()
			blStartRange := xtime.Range{Start: blStartTime, End: blStartTime}
			if !tr.Contains(blStartRange) {
				instrument.EmitAndLogInvariantViolation(r.opts.InstrumentOptions(), func(l *zap.Logger) {
					l.With(
						zap.Time("blockStart", blockStart.ToTime()),
						zap.String("namespace", nsMeta.ID().String()),
						zap.Uint32("shard", shard.ID()),
					).Error("repair received replica metadata for unrequested blockStart")
				})
				continue
			}

			for _, replicaMetadata := range replicaMetadataBlocks.Metadata() {
				if replicaMetadata.Host.ID() == session.Origin().ID() {
					// Don't request blocks for self metadata.
					continue
				}
				metadatas = append(metadatas, replicaMetadata)
			}
		}
	}

	perSeriesReplicaIter, err := session.FetchBlocksFromPeers(nsMeta, shard.ID(), level, metadatas, rsOpts)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	// TODO(rartoul): Copying the IDs for the purposes of the map key is wasteful. Considering using
	// SetUnsafe or marking as NoFinalize() and making the map check IsNoFinalize().
	numMismatchSeries := metadataRes.ChecksumDifferences.Series().Len()
	results := result.NewShardResult(numMismatchSeries, rsOpts)
	for perSeriesReplicaIter.Next() {
		_, id, block := perSeriesReplicaIter.Current()
		// TODO(rartoul): Handle tags in both branches: https://github.com/m3db/m3/issues/1848
		if existing, ok := results.BlockAt(id, block.StartTime()); ok {
			if err := existing.Merge(block); err != nil {
				return repair.MetadataComparisonResult{}, err
			}
		} else {
			results.AddBlock(id, ident.Tags{}, block)
		}
	}

	if err := shard.Load(results.AllSeries()); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	r.recordFn(nsCtx.ID, shard, metadataRes)

	return metadataRes, nil
}

func (r shardRepairer) recordDifferences(
	namespace ident.ID,
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
	LastAttempt time.Time
	Status      repairStatus
}

type namespaceRepairStateByTime map[xtime.UnixNano]repairState

// NB(r): This uses a map[string]element instead of a generated map for
// native ident.ID keys, this was because the call frequency is very low
// it's not in the hot path so casting ident.ID to string isn't too expensive
// and this data structure may very well change soon with a refactor of the
// background repair in the works.
type repairStatesByNs map[string]namespaceRepairStateByTime

func newRepairStates() repairStatesByNs {
	return make(repairStatesByNs)
}

func (r repairStatesByNs) repairStates(
	namespace ident.ID,
	t time.Time,
) (repairState, bool) {
	var rs repairState

	nsRepairState, ok := r[namespace.String()]
	if !ok {
		return rs, false
	}

	rs, ok = nsRepairState[xtime.ToUnixNano(t)]
	return rs, ok
}

func (r repairStatesByNs) setRepairState(
	namespace ident.ID,
	t time.Time,
	state repairState,
) {
	nsRepairState, ok := r[namespace.String()]
	if !ok {
		nsRepairState = make(namespaceRepairStateByTime)
		r[namespace.String()] = nsRepairState
	}
	nsRepairState[xtime.ToUnixNano(t)] = state
}

// NB(prateek): dbRepairer.Repair(...) guarantees atomicity of execution, so all other
// state does not need to be thread safe. One exception - `dbRepairer.closed` is used
// for early termination if `dbRepairer.Stop()` is called during a repair, so we guard
// it with a mutex.
type dbRepairer struct {
	database         database
	opts             Options
	ropts            repair.Options
	shardRepairer    databaseShardRepairer
	repairStatesByNs repairStatesByNs

	repairFn            repairFn
	sleepFn             sleepFn
	nowFn               clock.NowFn
	logger              *zap.Logger
	repairCheckInterval time.Duration
	scope               tally.Scope
	status              tally.Gauge

	closedLock sync.Mutex
	running    int32
	closed     bool
}

func newDatabaseRepairer(database database, opts Options) (databaseRepairer, error) {
	var (
		nowFn = opts.ClockOptions().NowFn()
		scope = opts.InstrumentOptions().MetricsScope().SubScope("repair")
		ropts = opts.RepairOptions()
	)
	if ropts == nil {
		return nil, errNoRepairOptions
	}
	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	shardRepairer := newShardRepairer(opts, ropts)

	r := &dbRepairer{
		database:            database,
		opts:                opts,
		ropts:               ropts,
		shardRepairer:       shardRepairer,
		repairStatesByNs:    newRepairStates(),
		sleepFn:             time.Sleep,
		nowFn:               nowFn,
		logger:              opts.InstrumentOptions().Logger(),
		repairCheckInterval: ropts.RepairCheckInterval(),
		scope:               scope,
		status:              scope.Gauge("repair"),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) run() {
	for {
		r.closedLock.Lock()
		closed := r.closed
		r.closedLock.Unlock()

		if closed {
			break
		}

		r.sleepFn(r.repairCheckInterval)

		if err := r.repairFn(); err != nil {
			r.logger.Error("error repairing database", zap.Error(err))
		}
	}
}

func (r *dbRepairer) namespaceRepairTimeRange(ns databaseNamespace) xtime.Range {
	var (
		now       = r.nowFn()
		rtopts    = ns.Options().RetentionOptions()
		blockSize = rtopts.BlockSize()
		start     = now.Add(-rtopts.RetentionPeriod()).Truncate(blockSize)
		end       = now.Add(-rtopts.BufferPast()).Truncate(blockSize)
	)
	return xtime.Range{Start: start, End: end}
}

func (r *dbRepairer) Start() {
	go r.run()
}

func (r *dbRepairer) Stop() {
	r.closedLock.Lock()
	r.closed = true
	r.closedLock.Unlock()
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
	namespaces, err := r.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	for _, n := range namespaces {
		repairRange := r.namespaceRepairTimeRange(n)
		blockSize := n.Options().RetentionOptions().BlockSize()

		// Iterating backwards will be inclusve on the end and exclusive on the start, but we
		// want the opposite behavior with the existing range so subtract a blocksize on each
		// end to make it inclusive on the original start and exclusive on the original end.
		repairRange.Start = repairRange.Start.Add(-blockSize)
		repairRange.End = repairRange.End.Add(-blockSize)

		numUnrepairedBlocks := 0
		repairRange.IterateBackwards(blockSize, func(blockStart time.Time) bool {
			repairState, ok := r.repairStatesByNs.repairStates(n.ID(), blockStart)
			if !ok || repairState.Status != repairSuccess {
				numUnrepairedBlocks++
			}

			return true
		})

		r.scope.Tagged(map[string]string{
			"namespace": n.ID().String(),
		}).Gauge("num-unrepaired-blocks").Update(float64(numUnrepairedBlocks))

		if numUnrepairedBlocks > 0 {
			repairRange.IterateBackwards(blockSize, func(blockStart time.Time) bool {
				repairState, ok := r.repairStatesByNs.repairStates(n.ID(), blockStart)
				if !ok || repairState.Status != repairSuccess {
					if err := r.repairNamespaceBlockstart(n, blockStart); err != nil {
						multiErr = multiErr.Add(err)
					}
					// Only repair one block per namespace per iteration.
					return false
				}
				return true
			})

			continue
		}

		var (
			leastRecentlyRepairedBlockStart               time.Time
			leastRecentlyRepairedBlockStartLastRepairTime time.Time
			maxSecondsSinceLastBlockRepair                = r.scope.Tagged(map[string]string{
				"namespace": n.ID().String(),
			}).Gauge("max-seconds-since-last-block-repair")
		)
		repairRange.IterateBackwards(blockSize, func(blockStart time.Time) bool {
			repairState, ok := r.repairStatesByNs.repairStates(n.ID(), blockStart)
			if !ok {
				// Should never happen.
				instrument.EmitAndLogInvariantViolation(r.opts.InstrumentOptions(), func(l *zap.Logger) {
					l.With(
						zap.Time("blockStart", blockStart),
						zap.String("namespace", n.ID().String()),
					).Error("missing repair state in all-blocks-are-repaired branch")
				})
				return true
			}

			if leastRecentlyRepairedBlockStart.IsZero() || repairState.LastAttempt.Before(leastRecentlyRepairedBlockStartLastRepairTime) {
				leastRecentlyRepairedBlockStart = blockStart
				leastRecentlyRepairedBlockStartLastRepairTime = repairState.LastAttempt
			}
			return true
		})

		secondsSinceLastRepair := r.nowFn().Sub(leastRecentlyRepairedBlockStartLastRepairTime).Seconds()
		maxSecondsSinceLastBlockRepair.Update(secondsSinceLastRepair)
		if err := r.repairNamespaceBlockstart(n, leastRecentlyRepairedBlockStart); err != nil {
			multiErr = multiErr.Add(err)
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

func (r *dbRepairer) repairNamespaceBlockstart(n databaseNamespace, blockStart time.Time) error {
	var (
		blockSize   = n.Options().RetentionOptions().BlockSize()
		repairRange = xtime.Range{Start: blockStart, End: blockStart.Add(blockSize)}
		repairTime  = r.nowFn()
	)
	if err := r.repairNamespaceWithTimeRange(n, repairRange); err != nil {
		r.markRepairAttempt(n.ID(), blockStart, repairTime, repairFailed)
		return err
	}

	r.markRepairAttempt(n.ID(), blockStart, repairTime, repairSuccess)
	return nil
}

func (r *dbRepairer) repairNamespaceWithTimeRange(n databaseNamespace, tr xtime.Range) error {
	if err := n.Repair(r.shardRepairer, tr); err != nil {
		return fmt.Errorf("namespace %s failed to repair time range %v: %v", n.ID().String(), tr, err)
	}

	return nil
}

func (r *dbRepairer) markRepairAttempt(
	namespace ident.ID,
	blockStart time.Time,
	repairTime time.Time,
	repairStatus repairStatus) {
	repairState, _ := r.repairStatesByNs.repairStates(namespace, blockStart)
	repairState.Status = repairStatus
	repairState.LastAttempt = repairTime
	r.repairStatesByNs.setRepairState(namespace, blockStart, repairState)
}

var noOpRepairer databaseRepairer = repairerNoOp{}

type repairerNoOp struct{}

func newNoopDatabaseRepairer() databaseRepairer { return noOpRepairer }

func (r repairerNoOp) Start()        {}
func (r repairerNoOp) Stop()         {}
func (r repairerNoOp) Repair() error { return nil }
func (r repairerNoOp) Report()       {}

func (r shardRepairer) shadowCompare(
	start time.Time,
	end time.Time,
	localMetadataBlocks block.FetchBlocksMetadataResults,
	session client.AdminSession,
	shard databaseShard,
	nsCtx namespace.Context,
) error {
	dice, err := dice.NewDice(r.rpopts.DebugShadowComparisonsPercentage())
	if err != nil {
		return fmt.Errorf("err creating shadow comparison dice: %v", err)
	}

	var localM, peerM *dynamic.Message
	if nsCtx.Schema != nil {
		// Only required if a schema (proto feature) is present. Reset between uses.
		localM = dynamic.NewMessage(nsCtx.Schema.Get().MessageDescriptor)
		peerM = dynamic.NewMessage(nsCtx.Schema.Get().MessageDescriptor)
	}

	tmpCtx := context.NewContext()
	compareResultFunc := func(result block.FetchBlocksMetadataResult) error {
		seriesID := result.ID
		peerSeriesIter, err := session.Fetch(nsCtx.ID, seriesID, start, end)
		if err != nil {
			return err
		}
		defer peerSeriesIter.Close()

		tmpCtx.Reset()
		defer tmpCtx.BlockingClose()

		unfilteredLocalSeriesDataBlocks, err := shard.ReadEncoded(tmpCtx, seriesID, start, end, nsCtx)
		if err != nil {
			return err
		}
		localSeriesDataBlocks, err := xio.FilterEmptyBlockReadersInPlaceSliceOfSlices(unfilteredLocalSeriesDataBlocks)
		if err != nil {
			return err
		}

		localSeriesSliceOfSlices := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(localSeriesDataBlocks)
		localSeriesIter := r.opts.MultiReaderIteratorPool().Get()
		localSeriesIter.ResetSliceOfSlices(localSeriesSliceOfSlices, nsCtx.Schema)

		var (
			i             = 0
			foundMismatch = false
		)
		for localSeriesIter.Next() {
			if !peerSeriesIter.Next() {
				r.logger.Error(
					"series had next locally, but not from peers",
					zap.String("namespace", nsCtx.ID.String()),
					zap.Time("start", start),
					zap.Time("end", end),
					zap.String("series", seriesID.String()),
					zap.Error(peerSeriesIter.Err()),
				)
				foundMismatch = true
				break
			}

			localDP, localUnit, localAnnotation := localSeriesIter.Current()
			peerDP, peerUnit, peerAnnotation := peerSeriesIter.Current()

			if !localDP.Equal(peerDP) {
				r.logger.Error(
					"datapoints did not match",
					zap.Int("index", i),
					zap.Any("local", localDP),
					zap.Any("peer", peerDP),
				)
				foundMismatch = true
				break
			}

			if localUnit != peerUnit {
				r.logger.Error(
					"units did not match",
					zap.Int("index", i),
					zap.Int("local", int(localUnit)),
					zap.Int("peer", int(peerUnit)),
				)
				foundMismatch = true
				break
			}

			if nsCtx.Schema == nil {
				// Remaining shadow logic is proto-specific.
				continue
			}

			err = localM.Unmarshal(localAnnotation)
			if err != nil {
				r.logger.Error(
					"Unable to unmarshal local annotation",
					zap.Int("index", i),
					zap.Error(err),
				)
				foundMismatch = true
				break
			}

			err = peerM.Unmarshal(peerAnnotation)
			if err != nil {
				r.logger.Error(
					"Unable to unmarshal peer annotation",
					zap.Int("index", i),
					zap.Error(err),
				)
				foundMismatch = true
				break
			}

			if !dynamic.Equal(localM, peerM) {
				r.logger.Error(
					"Local message does not equal peer message",
					zap.Int("index", i),
					zap.String("local", localM.String()),
					zap.String("peer", peerM.String()),
				)
				foundMismatch = true
				break
			}

			if !bytes.Equal(localAnnotation, peerAnnotation) {
				r.logger.Error(
					"Local message equals peer message, but annotations do not match",
					zap.Int("index", i),
					zap.String("local", string(localAnnotation)),
					zap.String("peer", string(peerAnnotation)),
				)
				foundMismatch = true
				break
			}

			i++
		}

		if localSeriesIter.Err() != nil {
			r.logger.Error(
				"Local series iterator experienced an error",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start),
				zap.Time("end", end),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
				zap.Error(localSeriesIter.Err()),
			)
		} else if foundMismatch {
			r.logger.Error(
				"Found mismatch between series",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start),
				zap.Time("end", end),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
			)
		} else {
			r.logger.Debug(
				"All values for series match",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start),
				zap.Time("end", end),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
			)
		}

		return nil
	}

	for _, result := range localMetadataBlocks.Results() {
		if !dice.Roll() {
			continue
		}

		if err := compareResultFunc(result); err != nil {
			return err
		}
	}

	return nil
}
