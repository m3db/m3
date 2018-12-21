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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

// bootstrapProcessProvider is the bootstrapping process provider.
type bootstrapProcessProvider struct {
	sync.RWMutex
	processOpts          ProcessOptions
	resultOpts           result.Options
	log                  xlog.Logger
	bootstrapperProvider BootstrapperProvider
}

type bootstrapRunType string

const (
	bootstrapDataRunType  = bootstrapRunType("bootstrap-data")
	bootstrapIndexRunType = bootstrapRunType("bootstrap-index")
)

// NewProcessProvider creates a new bootstrap process provider.
func NewProcessProvider(
	bootstrapperProvider BootstrapperProvider,
	processOpts ProcessOptions,
	resultOpts result.Options,
) (ProcessProvider, error) {
	if err := processOpts.Validate(); err != nil {
		return nil, err
	}

	return &bootstrapProcessProvider{
		processOpts:          processOpts,
		resultOpts:           resultOpts,
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

	initialTopologyState, err := b.newInitialTopologyState()
	if err != nil {
		return nil, err
	}

	return bootstrapProcess{
		processOpts:          b.processOpts,
		resultOpts:           b.resultOpts,
		nowFn:                b.resultOpts.ClockOptions().NowFn(),
		log:                  b.log,
		bootstrapper:         bootstrapper,
		initialTopologyState: initialTopologyState,
	}, nil
}

func (b *bootstrapProcessProvider) newInitialTopologyState() (*topology.StateSnapshot, error) {
	topoMap, err := b.processOpts.TopologyMapProvider().TopologyMap()
	if err != nil {
		return nil, err
	}

	var (
		hostShardSets = topoMap.HostShardSets()
		topologyState = &topology.StateSnapshot{
			Origin:           b.processOpts.Origin(),
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
	nowFn                clock.NowFn
	log                  xlog.Logger
	bootstrapper         Bootstrapper
	initialTopologyState *topology.StateSnapshot
}

func (b bootstrapProcess) Run(
	start time.Time,
	namespace namespace.Metadata,
	shards []uint32,
) (ProcessResult, error) {
	dataResult, err := b.bootstrapData(start, namespace, shards)
	if err != nil {
		return ProcessResult{}, err
	}

	indexResult, err := b.bootstrapIndex(start, namespace, shards)
	if err != nil {
		return ProcessResult{}, err
	}

	return ProcessResult{
		DataResult:  dataResult,
		IndexResult: indexResult,
	}, nil
}

func (b bootstrapProcess) bootstrapData(
	at time.Time,
	namespace namespace.Metadata,
	shards []uint32,
) (result.DataBootstrapResult, error) {
	bootstrapResult := result.NewDataBootstrapResult()
	ropts := namespace.Options().RetentionOptions()
	targetRanges := b.targetRangesForData(at, ropts)
	for _, target := range targetRanges {
		logFields := b.logFields(bootstrapDataRunType, namespace,
			shards, target.Range)
		b.logBootstrapRun(logFields)

		begin := b.nowFn()
		shardsTimeRanges := b.newShardTimeRanges(target.Range, shards)
		res, err := b.bootstrapper.BootstrapData(namespace,
			shardsTimeRanges, target.RunOptions)

		b.logBootstrapResult(logFields, err, begin)
		if err != nil {
			return nil, err
		}

		bootstrapResult = result.MergedDataBootstrapResult(bootstrapResult, res)
	}

	return bootstrapResult, nil
}

func (b bootstrapProcess) bootstrapIndex(
	at time.Time,
	namespace namespace.Metadata,
	shards []uint32,
) (result.IndexBootstrapResult, error) {
	bootstrapResult := result.NewIndexBootstrapResult()
	ropts := namespace.Options().RetentionOptions()
	idxopts := namespace.Options().IndexOptions()
	if !idxopts.Enabled() {
		// NB(r): If indexing not enable we just return an empty result
		return result.NewIndexBootstrapResult(), nil
	}

	targetRanges := b.targetRangesForIndex(at, ropts, idxopts)
	for _, target := range targetRanges {
		logFields := b.logFields(bootstrapIndexRunType, namespace,
			shards, target.Range)
		b.logBootstrapRun(logFields)

		begin := b.nowFn()
		shardsTimeRanges := b.newShardTimeRanges(target.Range, shards)
		res, err := b.bootstrapper.BootstrapIndex(namespace,
			shardsTimeRanges, target.RunOptions)

		b.logBootstrapResult(logFields, err, begin)
		if err != nil {
			return nil, err
		}

		bootstrapResult = result.MergedIndexBootstrapResult(bootstrapResult, res)
	}

	return bootstrapResult, nil
}

func (b bootstrapProcess) logFields(
	runType bootstrapRunType,
	namespace namespace.Metadata,
	shards []uint32,
	window xtime.Range,
) []xlog.Field {
	return []xlog.Field{
		xlog.NewField("run", string(runType)),
		xlog.NewField("bootstrapper", b.bootstrapper.String()),
		xlog.NewField("namespace", namespace.ID().String()),
		xlog.NewField("numShards", len(shards)),
		xlog.NewField("from", window.Start.String()),
		xlog.NewField("to", window.End.String()),
		xlog.NewField("range", window.End.Sub(window.Start).String()),
	}
}

func (b bootstrapProcess) newShardTimeRanges(
	window xtime.Range,
	shards []uint32,
) result.ShardTimeRanges {
	shardsTimeRanges := make(result.ShardTimeRanges, len(shards))
	ranges := xtime.NewRanges(window)
	for _, s := range shards {
		shardsTimeRanges[s] = ranges
	}
	return shardsTimeRanges
}

func (b bootstrapProcess) logBootstrapRun(
	logFields []xlog.Field,
) {
	b.log.WithFields(logFields...).Infof("bootstrapping shards for range starting")
}

func (b bootstrapProcess) logBootstrapResult(
	logFields []xlog.Field,
	err error,
	begin time.Time,
) {
	logFields = append(logFields, xlog.NewField("took", b.nowFn().Sub(begin).String()))
	if err != nil {
		logFields = append(logFields, xlog.NewField("error", err.Error()))
		b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed with error")
		return
	}

	b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed successfully")
}

func (b bootstrapProcess) targetRangesForData(
	at time.Time,
	ropts retention.Options,
) []TargetRange {
	return b.targetRanges(at, targetRangesOptions{
		retentionPeriod:       ropts.RetentionPeriod(),
		futureRetentionPeriod: ropts.FutureRetentionPeriod(),
		blockSize:             ropts.BlockSize(),
		bufferPast:            ropts.BufferPast(),
		bufferFuture:          ropts.BufferFuture(),
	})
}

func (b bootstrapProcess) targetRangesForIndex(
	at time.Time,
	ropts retention.Options,
	idxopts namespace.IndexOptions,
) []TargetRange {
	return b.targetRanges(at, targetRangesOptions{
		retentionPeriod:       ropts.RetentionPeriod(),
		futureRetentionPeriod: ropts.FutureRetentionPeriod(),
		blockSize:             idxopts.BlockSize(),
		bufferPast:            ropts.BufferPast(),
		bufferFuture:          ropts.BufferFuture(),
	})
}

type targetRangesOptions struct {
	retentionPeriod       time.Duration
	futureRetentionPeriod time.Duration
	blockSize             time.Duration
	bufferPast            time.Duration
	bufferFuture          time.Duration
}

func (b bootstrapProcess) targetRanges(
	at time.Time,
	opts targetRangesOptions,
) []TargetRange {
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

	// NB(r): We want the large initial time range bootstrapped to
	// bootstrap with persistence so we don't keep the full raw
	// data in process until we finish bootstrapping which could
	// cause the process to OOM.
	return []TargetRange{
		{
			Range: xtime.Range{Start: start, End: midPoint},
			RunOptions: b.newRunOptions().SetPersistConfig(PersistConfig{
				Enabled: true,
				// These blocks are no longer active, so we want to flush them
				// to disk as we receive them so that we don't hold too much
				// data in memory at once.
				FileSetType: persist.FileSetFlushType,
			}),
		},
		{
			Range: xtime.Range{Start: midPoint, End: cutover},
			RunOptions: b.newRunOptions().SetPersistConfig(PersistConfig{
				Enabled: true,
				// These blocks are still active so we'll have to keep them
				// in memory, but we want to snapshot them as we receive them
				// so that once bootstrapping completes we can still recover
				// from just the commit log bootstrapper.
				FileSetType: persist.FileSetSnapshotType,
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
