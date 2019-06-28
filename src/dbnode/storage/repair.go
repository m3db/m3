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
	"math/rand"
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
		accumLocalMetadata block.FetchBlocksMetadataResults
		pageToken          PageToken
	)
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

		if accumLocalMetadata == nil {
			// Set if not exist.
			accumLocalMetadata = currLocalMetadata
		} else {
			// Otherwise merge.
			if currLocalMetadata != nil {
				for _, result := range currLocalMetadata.Results() {
					accumLocalMetadata.Add(result)
				}
			}
		}

		if pageToken == nil {
			break
		}
	}

	if r.rpopts.DebugShadowComparisonsEnabled() {
		// Shadow comparison is mostly a debug feature that can be used to test new builds and diagnose
		// issues with the repair feature. It should not be enabled for production use-cases.
		err := r.shadowCompare(ctx, start, end, accumLocalMetadata, session, shard, nsCtx)
		if err != nil {
			r.logger.Error(
				"Shadow compare failed",
				zap.Error(err))
			return repair.MetadataComparisonResult{}, err
		}
	}

	localIter := block.NewFilteredBlocksMetadataIter(accumLocalMetadata)
	err = metadata.AddLocalMetadata(origin, localIter)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	// Add peer metadata
	level := r.rpopts.RepairConsistencyLevel()
	peerIter, err := session.FetchBlocksMetadataFromPeers(nsCtx.ID, shard.ID(), start, end,
		level, result.NewOptions())
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	metadataRes := metadata.Compare()

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
	Status      repairStatus
	NumFailures int
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
	ropts            repair.Options
	shardRepairer    databaseShardRepairer
	repairStatesByNs repairStatesByNs

	repairFn            repairFn
	sleepFn             sleepFn
	nowFn               clock.NowFn
	logger              *zap.Logger
	repairInterval      time.Duration
	repairTimeOffset    time.Duration
	repairTimeJitter    time.Duration
	repairCheckInterval time.Duration
	repairMaxRetries    int
	status              tally.Gauge

	closedLock sync.Mutex
	running    int32
	closed     bool
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

	shardRepairer := newShardRepairer(opts, ropts)

	var jitter time.Duration
	if repairJitter := ropts.RepairTimeJitter(); repairJitter > 0 {
		src := rand.NewSource(nowFn().UnixNano())
		jitter = time.Duration(float64(repairJitter) * (float64(src.Int63()) / float64(math.MaxInt64)))
	}

	r := &dbRepairer{
		database:            database,
		ropts:               ropts,
		shardRepairer:       shardRepairer,
		repairStatesByNs:    newRepairStates(),
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
		r.closedLock.Lock()
		closed := r.closed
		r.closedLock.Unlock()

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
		if intervalStart.Equal(curIntervalStart) {
			continue
		}

		curIntervalStart = intervalStart
		if err := r.repairFn(); err != nil {
			r.logger.Error("error repairing database", zap.Error(err))
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

	targetRanges := xtime.NewRanges(xtime.Range{Start: start, End: end})
	for tNano := range r.repairStatesByNs[ns.ID().String()] {
		t := tNano.ToTime()
		if !r.needsRepair(ns.ID(), t) {
			targetRanges = targetRanges.RemoveRange(xtime.Range{Start: t, End: t.Add(blockSize)})
		}
	}

	return targetRanges
}

func (r *dbRepairer) needsRepair(ns ident.ID, t time.Time) bool {
	repairState, exists := r.repairStatesByNs.repairStates(ns, t)
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
		repairState, _ := r.repairStatesByNs.repairStates(n.ID(), t)
		if err == nil {
			repairState.Status = repairSuccess
		} else {
			repairState.Status = repairFailed
			repairState.NumFailures++
		}
		r.repairStatesByNs.setRepairState(n.ID(), t, repairState)
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

func (r shardRepairer) shadowCompare(
	ctx context.Context,
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
	compareResultFunc := func(result block.FetchBlocksMetadataResult) error {
		seriesID := result.ID
		peerSeriesIter, err := session.Fetch(nsCtx.ID, seriesID, start, end)
		if err != nil {
			return err
		}
		defer peerSeriesIter.Close()

		// TODO: Use local context here.
		localSeriesDataBlocks, err := shard.ReadEncoded(ctx, seriesID, start, end, nsCtx)
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

			if !localDP.Timestamp.Equal(peerDP.Timestamp) {
				r.logger.Error(
					"timestamps did not match",
					zap.Int("index", i),
					zap.Time("local", localDP.Timestamp),
					zap.Time("peer", peerDP.Timestamp),
				)
				foundMismatch = true
				break
			}

			if localDP.Value != peerDP.Value {
				r.logger.Error(
					"Values did not match",
					zap.Int("index", i),
					zap.Float64("local", localDP.Value),
					zap.Float64("peer", peerDP.Value),
				)
				foundMismatch = true
				break
			}

			if localUnit != peerUnit {
				r.logger.Error(
					"Values did not match",
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
			r.logger.Info(
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
