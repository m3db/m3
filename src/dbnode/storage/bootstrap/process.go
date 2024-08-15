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

package bootstrap

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errNoOrigin      = errors.New("no origin set for initial topology state")
	errNoTopologyMap = errors.New("no topology map set for initial topology state")
)

// bootstrapProcessProvider is the bootstrapping process provider.
type bootstrapProcessProvider struct {
	sync.RWMutex
	processOpts          ProcessOptions
	resultOpts           result.Options
	fsOpts               fs.Options
	log                  *zap.Logger
	bootstrapperProvider BootstrapperProvider
}

// ErrFileSetSnapshotTypeRangeAdvanced is an error of bootstrap time ranges for snapshot-type
// blocks advancing during the bootstrap
var ErrFileSetSnapshotTypeRangeAdvanced = errors.New(
	"retrying bootstrap in order to recalculate time ranges (this is OK)")

// NewProcessProvider creates a new bootstrap process provider.
func NewProcessProvider(
	bootstrapperProvider BootstrapperProvider,
	processOpts ProcessOptions,
	resultOpts result.Options,
	fsOpts fs.Options,
) (ProcessProvider, error) {
	if err := processOpts.Validate(); err != nil {
		return nil, err
	}

	return &bootstrapProcessProvider{
		processOpts:          processOpts,
		resultOpts:           resultOpts,
		fsOpts:               fsOpts,
		log:                  resultOpts.InstrumentOptions().Logger(),
		bootstrapperProvider: bootstrapperProvider,
	}, nil
}

func (b *bootstrapProcessProvider) SetBootstrapperProvider(bootstrapperProvider BootstrapperProvider) {
	b.Lock()
	defer b.Unlock()
	b.bootstrapperProvider = bootstrapperProvider
}

func (b *bootstrapProcessProvider) BootstrapperProvider() BootstrapperProvider {
	b.RLock()
	defer b.RUnlock()
	return b.bootstrapperProvider
}

func (b *bootstrapProcessProvider) Provide() (Process, error) {
	b.RLock()
	defer b.RUnlock()
	bootstrapper, err := b.bootstrapperProvider.Provide()
	if err != nil {
		return nil, err
	}

	topoMap, err := b.processOpts.TopologyMapProvider().TopologyMap()
	if err != nil {
		return nil, err
	}

	origin := b.processOpts.Origin()
	initialTopologyState, err := newInitialTopologyState(origin, topoMap)
	if err != nil {
		return nil, err
	}

	return bootstrapProcess{
		processOpts:          b.processOpts,
		resultOpts:           b.resultOpts,
		fsOpts:               b.fsOpts,
		nowFn:                b.resultOpts.ClockOptions().NowFn(),
		log:                  b.log,
		bootstrapper:         bootstrapper,
		initialTopologyState: initialTopologyState,
	}, nil
}

func newInitialTopologyState(
	origin topology.Host,
	topoMap topology.Map,
) (*topology.StateSnapshot, error) {
	if origin == nil {
		return nil, errNoOrigin
	}
	if topoMap == nil {
		return nil, errNoTopologyMap
	}

	var (
		hostShardSets = topoMap.HostShardSets()
		topologyState = &topology.StateSnapshot{
			Origin:           origin,
			MajorityReplicas: topoMap.MajorityReplicas(),
			ShardStates:      topology.ShardStates{},
		}
	)

	for _, hostShardSet := range hostShardSets {
		for _, currShard := range hostShardSet.ShardSet().All() {
			shardID := topology.ShardID(currShard.ID())
			existing, ok := topologyState.ShardStates[shardID]
			if !ok {
				existing = map[topology.HostID]topology.HostShardState{}
				topologyState.ShardStates[shardID] = existing
			}

			hostID := topology.HostID(hostShardSet.Host().ID())
			existing[hostID] = topology.HostShardState{
				Host:       hostShardSet.Host(),
				ShardState: currShard.State(),
			}
		}
	}

	return topologyState, nil
}

type bootstrapProcess struct {
	processOpts          ProcessOptions
	resultOpts           result.Options
	fsOpts               fs.Options
	nowFn                clock.NowFn
	log                  *zap.Logger
	bootstrapper         Bootstrapper
	initialTopologyState *topology.StateSnapshot
}

func (b bootstrapProcess) Run(
	ctx context.Context,
	at xtime.UnixNano,
	namespaces []ProcessNamespace,
) (NamespaceResults, error) {
	namespacesRunFirst := Namespaces{
		Namespaces: NewNamespacesMap(NamespacesMapOptions{}),
	}
	namespacesRunSecond := Namespaces{
		Namespaces: NewNamespacesMap(NamespacesMapOptions{}),
	}
	namespaceDetails := make([]NamespaceDetails, 0, len(namespaces))
	for _, namespace := range namespaces {
		var (
			nsOpts      = namespace.Metadata.Options()
			dataRanges  = b.targetRangesForData(at, nsOpts)
			indexRanges = b.targetRangesForIndex(at, nsOpts)
			firstRanges = b.newShardTimeRanges(
				dataRanges.firstRangeWithPersistTrue.Range,
				namespace.Shards,
			)
		)

		namespacesRunFirst.Namespaces.Set(namespace.Metadata.ID(), Namespace{
			Metadata:         namespace.Metadata,
			Shards:           namespace.Shards,
			DataAccumulator:  namespace.DataAccumulator,
			Hooks:            namespace.Hooks,
			DataTargetRange:  dataRanges.firstRangeWithPersistTrue,
			IndexTargetRange: indexRanges.firstRangeWithPersistTrue,
			DataRunOptions: NamespaceRunOptions{
				ShardTimeRanges:       firstRanges.Copy(),
				TargetShardTimeRanges: firstRanges.Copy(),
				RunOptions:            dataRanges.firstRangeWithPersistTrue.RunOptions,
			},
			IndexRunOptions: NamespaceRunOptions{
				ShardTimeRanges:       firstRanges.Copy(),
				TargetShardTimeRanges: firstRanges.Copy(),
				RunOptions:            indexRanges.firstRangeWithPersistTrue.RunOptions,
			},
		})
		secondRanges := b.newShardTimeRanges(
			dataRanges.secondRange.Range, namespace.Shards)
		namespacesRunSecond.Namespaces.Set(namespace.Metadata.ID(), Namespace{
			Metadata:         namespace.Metadata,
			Shards:           namespace.Shards,
			DataAccumulator:  namespace.DataAccumulator,
			Hooks:            namespace.Hooks,
			DataTargetRange:  dataRanges.secondRange,
			IndexTargetRange: indexRanges.secondRange,
			DataRunOptions: NamespaceRunOptions{
				ShardTimeRanges:       secondRanges.Copy(),
				TargetShardTimeRanges: secondRanges.Copy(),
				RunOptions:            dataRanges.secondRange.RunOptions,
			},
			IndexRunOptions: NamespaceRunOptions{
				ShardTimeRanges:       secondRanges.Copy(),
				TargetShardTimeRanges: secondRanges.Copy(),
				RunOptions:            indexRanges.secondRange.RunOptions,
			},
		})
		namespaceDetails = append(namespaceDetails, NamespaceDetails{
			Namespace: namespace.Metadata,
			Shards:    namespace.Shards,
		})
	}
	cache, err := NewCache(NewCacheOptions().
		SetFilesystemOptions(b.fsOpts).
		SetNamespaceDetails(namespaceDetails).
		SetInstrumentOptions(b.fsOpts.InstrumentOptions()))
	if err != nil {
		return NamespaceResults{}, err
	}

	var (
		bootstrapResult = NewNamespaceResults(namespacesRunFirst)
		namespacesToRun = []Namespaces{namespacesRunFirst, namespacesRunSecond}
		lastRunIndex    = len(namespacesToRun) - 1
	)
	for runIndex, namespaces := range namespacesToRun {
		for _, entry := range namespaces.Namespaces.Iter() {
			ns := entry.Value()

			// First determine if any shards that we are bootstrapping are
			// initializing and hence might need peer bootstrapping and if so
			// make sure the time ranges reflect the time window that should
			// be bootstrapped from peers (in case time has shifted considerably).
			if !b.shardsInitializingAny(ns.Shards) {
				// No shards initializing, don't need to run check to see if
				// time has shifted.
				continue
			}

			// If last run, check if ranges have advanced while bootstrapping previous ranges.
			// If yes, return an error to force a retry.
			if runIndex == lastRunIndex {
				var (
					now                = xtime.ToUnixNano(b.nowFn())
					nsOptions          = ns.Metadata.Options()
					upToDateDataRanges = b.targetRangesForData(now, nsOptions)
				)
				// Only checking data ranges. Since index blocks can only be a multiple of
				// data block size, the ranges for index could advance only if data ranges
				// have advanced, too (while opposite is not necessarily true)
				if !upToDateDataRanges.secondRange.Range.Equal(ns.DataTargetRange.Range) {
					upToDateIndexRanges := b.targetRangesForIndex(now, nsOptions)
					fields := b.logFields(ns.Metadata, ns.Shards,
						upToDateDataRanges.secondRange.Range,
						upToDateIndexRanges.secondRange.Range)
					b.log.Error("time ranges of snapshot-type blocks advanced", fields...)
					return NamespaceResults{}, ErrFileSetSnapshotTypeRangeAdvanced
				}
			}
		}

		res, err := b.runPass(ctx, namespaces, cache)
		if err != nil {
			return NamespaceResults{}, err
		}

		bootstrapResult = MergeNamespaceResults(bootstrapResult, res)
	}

	return bootstrapResult, nil
}

func (b bootstrapProcess) shardsInitializingAny(
	shards []uint32,
) bool {
	for _, value := range shards {
		shardID := topology.ShardID(value)
		hostShardStates, ok := b.initialTopologyState.ShardStates[shardID]
		if !ok {
			// This shard was not part of the topology when the bootstrapping
			// process began.
			continue
		}

		originID := topology.HostID(b.initialTopologyState.Origin.ID())
		originHostShardState, ok := hostShardStates[originID]
		if !ok {
			// This shard was not part of the origin's shard.
			continue
		}

		if originHostShardState.ShardState == shard.Initializing {
			return true
		}
	}

	return false
}

func (b bootstrapProcess) runPass(
	ctx context.Context,
	namespaces Namespaces,
	cache Cache,
) (NamespaceResults, error) {
	ctx, span, sampled := ctx.StartSampledTraceSpan(tracepoint.BootstrapProcessRun)
	defer span.Finish()

	i := 0
	for _, entry := range namespaces.Namespaces.Iter() {
		ns := entry.Value()
		idx := i
		i++

		if sampled {
			ext := fmt.Sprintf("[%d]", idx)
			span.LogFields(
				log.String("namespace"+ext, ns.Metadata.ID().String()),
				log.Int("shards"+ext, len(ns.Shards)),
				log.String("dataRange"+ext, ns.DataTargetRange.Range.String()),
				log.String("indexRange"+ext, ns.IndexTargetRange.Range.String()),
			)
		}

		logFields := b.logFields(ns.Metadata, ns.Shards,
			ns.DataTargetRange.Range, ns.IndexTargetRange.Range)
		b.logBootstrapRun(logFields)
	}

	begin := b.nowFn()
	res, err := b.bootstrapper.Bootstrap(ctx, namespaces, cache)
	took := b.nowFn().Sub(begin)
	if err != nil {
		b.log.Error("bootstrap process error",
			zap.Duration("took", took),
			zap.Error(err))
		return NamespaceResults{}, err
	}

	for _, entry := range namespaces.Namespaces.Iter() {
		namespace := entry.Value()
		nsID := namespace.Metadata.ID()

		result, ok := res.Results.Get(nsID)
		if !ok {
			return NamespaceResults{},
				fmt.Errorf("result missing for namespace: %v", nsID.String())
		}

		logFields := b.logFields(namespace.Metadata, namespace.Shards,
			namespace.DataTargetRange.Range, namespace.IndexTargetRange.Range)
		b.logBootstrapResult(result, logFields, took)
	}

	return res, nil
}

func (b bootstrapProcess) logFields(
	namespace namespace.Metadata,
	shards []uint32,
	dataTimeWindow xtime.Range,
	indexTimeWindow xtime.Range,
) []zapcore.Field {
	fields := []zapcore.Field{
		zap.String("bootstrapper", b.bootstrapper.String()),
		zap.Stringer("namespace", namespace.ID()),
		zap.Int("numShards", len(shards)),
		zap.Time("dataFrom", dataTimeWindow.Start.ToTime()),
		zap.Time("dataTo", dataTimeWindow.End.ToTime()),
		zap.Duration("dataRange", dataTimeWindow.End.Sub(dataTimeWindow.Start)),
	}
	if namespace.Options().IndexOptions().Enabled() {
		fields = append(fields,
			zap.Time("indexFrom", indexTimeWindow.Start.ToTime()),
			zap.Time("indexTo", indexTimeWindow.End.ToTime()),
			zap.Duration("indexRange", indexTimeWindow.End.Sub(indexTimeWindow.Start)),
		)
	}
	return fields
}

func (b bootstrapProcess) newShardTimeRanges(
	window xtime.Range,
	shards []uint32,
) result.ShardTimeRanges {
	shardsTimeRanges := result.NewShardTimeRanges()
	ranges := xtime.NewRanges(window)
	for _, s := range shards {
		shardsTimeRanges.Set(s, ranges)
	}
	return shardsTimeRanges
}

func (b bootstrapProcess) logBootstrapRun(
	logFields []zapcore.Field,
) {
	b.log.Info("bootstrap range starting", logFields...)
}

func (b bootstrapProcess) logBootstrapResult(
	result NamespaceResult,
	logFields []zapcore.Field,
	took time.Duration,
) {
	logFields = append(logFields,
		zap.Duration("took", took))
	if result.IndexResult != nil {
		logFields = append(logFields,
			zap.Int("numIndexBlocks", len(result.IndexResult.IndexResults())))
	}

	b.log.Info("bootstrap range completed", logFields...)
}

func (b bootstrapProcess) targetRangesForData(
	at xtime.UnixNano,
	nsOpts namespace.Options,
) targetRangesResult {
	ropts := nsOpts.RetentionOptions()
	return b.targetRanges(at, targetRangesOptions{
		retentionPeriod:       ropts.RetentionPeriod(),
		futureRetentionPeriod: ropts.FutureRetentionPeriod(),
		blockSize:             ropts.BlockSize(),
		bufferPast:            ropts.BufferPast(),
		bufferFuture:          ropts.BufferFuture(),
		snapshotEnabled:       nsOpts.SnapshotEnabled(),
	})
}

func (b bootstrapProcess) targetRangesForIndex(
	at xtime.UnixNano,
	nsOpts namespace.Options,
) targetRangesResult {
	ropts := nsOpts.RetentionOptions()
	return b.targetRanges(at, targetRangesOptions{
		retentionPeriod:       ropts.RetentionPeriod(),
		futureRetentionPeriod: ropts.FutureRetentionPeriod(),
		blockSize:             nsOpts.IndexOptions().BlockSize(),
		bufferPast:            ropts.BufferPast(),
		bufferFuture:          ropts.BufferFuture(),
		snapshotEnabled:       nsOpts.SnapshotEnabled(),
	})
}

type targetRangesOptions struct {
	retentionPeriod       time.Duration
	futureRetentionPeriod time.Duration
	blockSize             time.Duration
	bufferPast            time.Duration
	bufferFuture          time.Duration
	snapshotEnabled       bool
}

type targetRangesResult struct {
	firstRangeWithPersistTrue TargetRange
	secondRange               TargetRange
}

func (b bootstrapProcess) targetRanges(
	at xtime.UnixNano,
	opts targetRangesOptions,
) targetRangesResult {
	start := at.Add(-opts.retentionPeriod).
		Truncate(opts.blockSize)
	midPoint := at.
		Add(-opts.blockSize).
		Add(-opts.bufferPast).
		Truncate(opts.blockSize).
		// NB(r): Since "end" is exclusive we need to add a
		// an extra block size when specifying the end time.
		Add(opts.blockSize)
	cutover := at.Add(opts.bufferFuture).
		Truncate(opts.blockSize).
		Add(opts.blockSize)

	secondRangeFilesetType := persist.FileSetSnapshotType
	if !opts.snapshotEnabled {
		// NB: If snapshots are disabled for a namespace, we want to use flush type.
		secondRangeFilesetType = persist.FileSetFlushType
	}

	// NB(r): We want the large initial time range bootstrapped to
	// bootstrap with persistence so we don't keep the full raw
	// data in process until we finish bootstrapping which could
	// cause the process to OOM.
	return targetRangesResult{
		firstRangeWithPersistTrue: TargetRange{
			Range: xtime.Range{Start: start, End: midPoint},
			RunOptions: b.newRunOptions().SetPersistConfig(PersistConfig{
				Enabled: true,
				// These blocks are no longer active, so we want to flush them
				// to disk as we receive them so that we don't hold too much
				// data in memory at once.
				FileSetType: persist.FileSetFlushType,
			}),
		},
		secondRange: TargetRange{
			Range: xtime.Range{Start: midPoint, End: cutover},
			RunOptions: b.newRunOptions().SetPersistConfig(PersistConfig{
				Enabled: true,
				// These blocks are still active so we'll have to keep them
				// in memory, but we want to snapshot them as we receive them
				// so that once bootstrapping completes we can still recover
				// from just the commit log bootstrapper.
				FileSetType: secondRangeFilesetType,
			}),
		},
	}
}

func (b bootstrapProcess) newRunOptions() RunOptions {
	return NewRunOptions().
		SetCacheSeriesMetadata(
			b.processOpts.CacheSeriesMetadata(),
		).
		SetInitialTopologyState(b.initialTopologyState)
}

// NewNamespaces returns a new set of bootstrappable namespaces.
func NewNamespaces(
	namespaces []ProcessNamespace,
) Namespaces {
	namespacesMap := NewNamespacesMap(NamespacesMapOptions{})
	for _, ns := range namespaces {
		namespacesMap.Set(ns.Metadata.ID(), Namespace{
			Metadata:        ns.Metadata,
			Shards:          ns.Shards,
			DataAccumulator: ns.DataAccumulator,
		})
	}
	return Namespaces{
		Namespaces: namespacesMap,
	}
}

// NewNamespaceResults creates a
// namespace results map with an entry for each
// namespace spoecified by a namespaces map.
func NewNamespaceResults(
	namespaces Namespaces,
) NamespaceResults {
	resultsMap := NewNamespaceResultsMap(NamespaceResultsMapOptions{})
	for _, entry := range namespaces.Namespaces.Iter() {
		key := entry.Key()
		value := entry.Value()
		resultsMap.Set(key, NamespaceResult{
			Metadata:    value.Metadata,
			Shards:      value.Shards,
			DataResult:  result.NewDataBootstrapResult(),
			IndexResult: result.NewIndexBootstrapResult(),
		})
	}
	return NamespaceResults{
		Results: resultsMap,
	}
}

// MergeNamespaceResults merges two namespace results, this will mutate
// both a and b and return a merged copy of them reusing one of the results.
func MergeNamespaceResults(a, b NamespaceResults) NamespaceResults {
	for _, entry := range a.Results.Iter() {
		id := entry.Key()
		elem := entry.Value()
		other, ok := b.Results.Get(id)
		if !ok {
			continue
		}
		elem.DataResult = result.MergedDataBootstrapResult(elem.DataResult,
			other.DataResult)
		elem.IndexResult = result.MergedIndexBootstrapResult(elem.IndexResult,
			other.IndexResult)

		// Save back the merged results.
		a.Results.Set(id, elem)

		// Remove from b, then can directly add to a all non-merged results.
		b.Results.Delete(id)
	}
	// All overlapping between a and b have been merged, add rest to a.
	for _, entry := range b.Results.Iter() {
		a.Results.Set(entry.Key(), entry.Value())
	}
	return a
}
