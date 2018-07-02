// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/dbnode/serialize"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/matcher/cache"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/coreos/etcd/integration"
	"github.com/uber-go/tally"
)

const (
	initAllocTagsSliceCapacity     = 32
	shardSetID                     = uint32(0)
	instanceID                     = "downsampler_local"
	placementKVKey                 = "/placement"
	aggregationSuffixTag           = "aggregation"
	defaultStorageFlushConcurrency = 20000
)

var (
	numShards = runtime.NumCPU()

	errNoStorage               = errors.New("dynamic downsampling enabled with storage set")
	errNoRulesStore            = errors.New("dynamic downsampling enabled with rules store not set")
	errNoClockOptions          = errors.New("dynamic downsampling enabled with clock options not set")
	errNoInstrumentOptions     = errors.New("dynamic downsampling enabled with instrument options not set")
	errNoTagEncoderOptions     = errors.New("dynamic downsampling enabled with tag encoder options not set")
	errNoTagDecoderOptions     = errors.New("dynamic downsampling enabled with tag decoder options not set")
	errNoTagEncoderPoolOptions = errors.New("dynamic downsampling enabled with tag encoder pool options not set")
	errNoTagDecoderPoolOptions = errors.New("dynamic downsampling enabled with tag decoder pool options not set")
)

// Downsampler is a downsampler.
type Downsampler interface {
	NonePolicy() NonePolicy
	AggregationPolicy() AggregationPolicy
	MetricsAppender() MetricsAppender
}

// MetricsAppender is a metrics appender that can
// build a samples appender, only valid to use
// with a single caller at a time.
type MetricsAppender interface {
	AddTag(name, value string)
	SamplesAppender() (SamplesAppender, error)
	Reset()
	Finalize()
}

// SamplesAppender is a downsampling samples appender,
// that can only be called by a single caller at a time.
type SamplesAppender interface {
	AppendCounterSample(value int64) error
	AppendGaugeSample(value float64) error
}

// DownsamplerOptions is a set of required downsampler options.
type DownsamplerOptions struct {
	Storage                 storage.Storage
	StorageFlushConcurrency int
	RulesKVStore            kv.Store
	ClockOptions            clock.Options
	InstrumentOptions       instrument.Options
	TagEncoderOptions       serialize.TagEncoderOptions
	TagDecoderOptions       serialize.TagDecoderOptions
	TagEncoderPoolOptions   pool.ObjectPoolOptions
	TagDecoderPoolOptions   pool.ObjectPoolOptions
}

// Validate will validate the dynamic downsampling options.
func (o DownsamplerOptions) Validate() error {
	if o.Storage == nil {
		return errNoStorage
	}
	if o.RulesKVStore == nil {
		return errNoRulesStore
	}
	if o.ClockOptions == nil {
		return errNoClockOptions
	}
	if o.InstrumentOptions == nil {
		return errNoInstrumentOptions
	}
	if o.TagEncoderOptions == nil {
		return errNoTagEncoderOptions
	}
	if o.TagDecoderOptions == nil {
		return errNoTagDecoderOptions
	}
	if o.TagEncoderPoolOptions == nil {
		return errNoTagEncoderPoolOptions
	}
	if o.TagDecoderPoolOptions == nil {
		return errNoTagDecoderPoolOptions
	}
	return nil
}

type newAggregatorResult struct {
	aggregator aggregator.Aggregator

	clockOpts clock.Options

	matcher matcher.Matcher

	tagEncoderPool serialize.TagEncoderPool
	tagDecoderPool serialize.TagDecoderPool

	encodedTagsIteratorPool *encodedTagsIteratorPool
}

func (o DownsamplerOptions) newAggregator() (newAggregatorResult, error) {
	// Validate options first.
	if err := o.Validate(); err != nil {
		return newAggregatorResult{}, err
	}

	var (
		storageFlushConcurrency = defaultStorageFlushConcurrency
		rulesStore              = o.RulesKVStore
		clockOpts               = o.ClockOptions
		instrumentOpts          = o.InstrumentOptions
	)
	if o.StorageFlushConcurrency > 0 {
		storageFlushConcurrency = storageFlushConcurrency
	}

	// Configure rules options.
	tagEncoderPool := serialize.NewTagEncoderPool(o.TagEncoderOptions,
		o.TagEncoderPoolOptions)
	tagEncoderPool.Init()

	tagDecoderPool := serialize.NewTagDecoderPool(o.TagDecoderOptions,
		o.TagDecoderPoolOptions)
	tagDecoderPool.Init()

	sortedTagIteratorPool := newEncodedTagsIteratorPool(tagDecoderPool,
		o.TagDecoderPoolOptions)
	sortedTagIteratorPool.Init()

	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := sortedTagIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagsFilterOptions := filters.TagsFilterOptions{
		NameTagKey: metricNameTagName,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, sortedTagIteratorPool)
			if err != nil {
				return nil, nil, err
			}
			// ID is always the encoded tags for IDs in the downsampler
			tags := id
			return name, tags, nil
		},
		SortedTagIteratorFn: sortedTagIteratorFn,
	}

	isRollupIDFn := func(name []byte, tags []byte) bool {
		return isRollupID(tags, sortedTagIteratorPool)
	}

	newRollupIDProviderPool := newRollupIDProviderPool(tagEncoderPool,
		o.TagEncoderPoolOptions)
	newRollupIDProviderPool.Init()

	newRollupIDFn := func(name []byte, tagPairs []id.TagPair) []byte {
		rollupIDProvider := newRollupIDProviderPool.Get()
		id, err := rollupIDProvider.provide(tagPairs)
		if err != nil {
			panic(err) // Encoding should never fail
		}
		rollupIDProvider.finalize()
		return id
	}

	// Use default aggregation types, in future we can provide more configurability
	var defaultAggregationTypes aggregation.TypesConfiguration
	aggTypeOpts, err := defaultAggregationTypes.NewOptions(instrumentOpts)
	if err != nil {
		return newAggregatorResult{}, err
	}

	ruleSetOpts := rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOptions).
		SetNewRollupIDFn(newRollupIDFn).
		SetIsRollupIDFn(isRollupIDFn).
		SetAggregationTypesOptions(aggTypeOpts)

	opts := matcher.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetRuleSetOptions(ruleSetOpts).
		SetKVStore(rulesStore)

	cacheOpts := cache.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().SubScope("matcher-cache")))

	cache := cache.NewCache(cacheOpts)

	matcher, err := matcher.NewMatcher(cache, opts)
	if err != nil {
		return newAggregatorResult{}, err
	}

	aggregatorOpts := aggregator.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetAggregationTypesOptions(aggTypeOpts).
		SetMetricPrefix(nil).
		SetCounterPrefix(nil).
		SetGaugePrefix(nil).
		SetTimerPrefix(nil)

	shardSet := make([]shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).
			SetState(shard.Initializing).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
	}
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(instanceID).
		SetShards(shards).
		SetShardSetID(shardSetID)
	localPlacement := placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shards.AllIDs())
	stagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]placement.Placement{localPlacement})
	stagedPlacementProto, err := stagedPlacement.Proto()
	if err != nil {
		return newAggregatorResult{}, err
	}

	placementStore := mem.NewStore()
	_, err = placementStore.SetIfNotExists(placementKVKey, stagedPlacementProto)
	if err != nil {
		return newAggregatorResult{}, err
	}

	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKVKey).
		SetStagedPlacementStore(placementStore)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)
	placementManager := aggregator.NewPlacementManager(placementManagerOpts)
	aggregatorOpts = aggregatorOpts.SetPlacementManager(placementManager)

	// Set up flush times manager.
	flushTimesManagerOpts := aggregator.NewFlushTimesManagerOptions().
		SetFlushTimesStore(placementStore)
	flushTimesManager := aggregator.NewFlushTimesManager(flushTimesManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushTimesManager(flushTimesManager)

	// Set up election manager.
	leaderValue := instanceID
	campaignOpts, err := services.NewCampaignOptions()
	if err != nil {
		return newAggregatorResult{}, err
	}

	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	electionCluster := integration.NewClusterV3(nil, &integration.ClusterConfig{
		Size: 1,
	})

	sid := services.NewServiceID().
		SetEnvironment("production").
		SetName("downsampler").
		SetZone("embedded")

	eopts := services.NewElectionOptions()

	leaderOpts := leader.NewOptions().
		SetServiceID(sid).
		SetElectionOpts(eopts)

	leaderService, err := leader.NewService(electionCluster.RandClient(), leaderOpts)
	if err != nil {
		return newAggregatorResult{}, err
	}

	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager)
	electionManager := aggregator.NewElectionManager(electionManagerOpts)
	aggregatorOpts = aggregatorOpts.SetElectionManager(electionManager)

	// Set up flush manager.
	flushManagerOpts := aggregator.NewFlushManagerOptions().
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager)
	flushManager := aggregator.NewFlushManager(flushManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushManager(flushManager)

	flushWorkers := xsync.NewWorkerPool(storageFlushConcurrency)
	flushWorkers.Init()
	handler := newDownsamplerFlushHandler(o.Storage, sortedTagIteratorPool,
		flushWorkers, instrumentOpts)
	aggregatorOpts = aggregatorOpts.SetFlushHandler(handler)

	return newAggregatorResult{
		aggregator:              aggregator.NewAggregator(aggregatorOpts),
		matcher:                 matcher,
		tagEncoderPool:          tagEncoderPool,
		tagDecoderPool:          tagDecoderPool,
		encodedTagsIteratorPool: sortedTagIteratorPool,
	}, nil
}

type retentionResolution struct {
	retention  time.Duration
	resolution time.Duration
}

type downsamplerFlushHandler struct {
	sync.RWMutex
	storage                 storage.Storage
	encodedTagsIteratorPool *encodedTagsIteratorPool
	workerPool              xsync.WorkerPool
	instrumentOpts          instrument.Options
	metrics                 downsamplerFlushHandlerMetrics
}

type downsamplerFlushHandlerMetrics struct {
	flushSuccess tally.Counter
	flushErrors  tally.Counter
}

func newDownsamplerFlushHandlerMetrics(
	scope tally.Scope,
) downsamplerFlushHandlerMetrics {
	return downsamplerFlushHandlerMetrics{
		flushSuccess: scope.Counter("flush-success"),
		flushErrors:  scope.Counter("flush-errors"),
	}
}

func newDownsamplerFlushHandler(
	storage storage.Storage,
	encodedTagsIteratorPool *encodedTagsIteratorPool,
	workerPool xsync.WorkerPool,
	instrumentOpts instrument.Options,
) handler.Handler {
	scope := instrumentOpts.MetricsScope().SubScope("downsampler-flush-handler")
	return &downsamplerFlushHandler{
		storage:                 storage,
		encodedTagsIteratorPool: encodedTagsIteratorPool,
		workerPool:              workerPool,
		instrumentOpts:          instrumentOpts,
		metrics:                 newDownsamplerFlushHandlerMetrics(scope),
	}
}

func (h *downsamplerFlushHandler) NewWriter(
	scope tally.Scope,
) (writer.Writer, error) {
	return &downsamplerFlushHandlerWriter{
		ctx:     context.Background(),
		handler: h,
	}, nil
}

func (h *downsamplerFlushHandler) Close() {
}

type downsamplerFlushHandlerWriter struct {
	wg      sync.WaitGroup
	ctx     context.Context
	handler *downsamplerFlushHandler
}

func (w *downsamplerFlushHandlerWriter) Write(
	mp aggregated.ChunkedMetricWithStoragePolicy,
) error {
	w.wg.Add(1)
	w.handler.workerPool.Go(func() {
		defer w.wg.Done()

		logger := w.handler.instrumentOpts.Logger()

		iter := w.handler.encodedTagsIteratorPool.Get()
		iter.Reset(mp.ChunkedID.Data)
		expected := iter.tagDecoder.Remaining()
		if len(mp.ChunkedID.Suffix) > 0 {
			expected++
		}
		tags := make(models.Tags, expected)
		for iter.Next() {
			name, value := iter.Current()
			tags[string(name)] = string(value)
		}
		if len(mp.ChunkedID.Suffix) > 0 {
			tags[aggregationSuffixTag] = string(mp.ChunkedID.Suffix)
		}

		err := iter.Err()
		iter.Close()
		if err != nil {
			logger.Debugf("downsampler flush error preparing write: %v", err)
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		err = w.handler.storage.Write(w.ctx, &storage.WriteQuery{
			Tags: tags,
			Datapoints: ts.Datapoints{ts.Datapoint{
				Timestamp: time.Unix(0, mp.TimeNanos),
				Value:     mp.Value,
			}},
			Unit: mp.StoragePolicy.Resolution().Precision,
			Attributes: storage.Attributes{
				MetricsType: storage.AggregatedMetricsType,
				Retention:   mp.StoragePolicy.Retention().Duration(),
				Resolution:  mp.StoragePolicy.Resolution().Window,
			},
		})
		if err != nil {
			logger.Debugf("downsampler flush error failed write: %v", err)
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		w.handler.metrics.flushSuccess.Inc(1)
	})

	return nil
}

func (w *downsamplerFlushHandlerWriter) Flush() error {
	w.wg.Wait()
	return nil
}

func (w *downsamplerFlushHandlerWriter) Close() error {
	return nil
}

type downsampler struct {
	cfg        DownsamplingConfiguration
	aggregator newAggregatorResult
}

// NewDownsampler returns a new downsampler.
func NewDownsampler(
	cfg DownsamplingConfiguration,
	opts DownsamplerOptions,
) (Downsampler, error) {
	d := &downsampler{
		cfg: cfg,
	}

	if cfg.AggregationPolicy.Enabled {
		result, err := opts.newAggregator()
		if err != nil {
			return nil, err
		}
		d.aggregator = result
	}

	return d, nil
}

func (d *downsampler) NonePolicy() NonePolicy {
	return d.cfg.NonePolicy
}

func (d *downsampler) AggregationPolicy() AggregationPolicy {
	return d.cfg.AggregationPolicy
}

func (d *downsampler) MetricsAppender() MetricsAppender {
	return newMetricsAppender(metricsAppenderOptions{
		agg:                     d.aggregator.aggregator,
		clockOpts:               d.aggregator.clockOpts,
		tagEncoder:              d.aggregator.tagEncoderPool.Get(),
		matcher:                 d.aggregator.matcher,
		encodedTagsIteratorPool: d.aggregator.encodedTagsIteratorPool,
	})
}

func newMetricsAppender(opts metricsAppenderOptions) *metricsAppender {
	return &metricsAppender{
		metricsAppenderOptions: opts,
		tags:                 newTags(),
		multiSamplesAppender: newMultiSamplesAppender(),
	}
}

type metricsAppender struct {
	metricsAppenderOptions

	tags                 *tags
	multiSamplesAppender *multiSamplesAppender
}

type metricsAppenderOptions struct {
	agg                     aggregator.Aggregator
	clockOpts               clock.Options
	tagEncoder              serialize.TagEncoder
	matcher                 matcher.Matcher
	encodedTagsIteratorPool *encodedTagsIteratorPool
}

func (a *metricsAppender) AddTag(name, value string) {
	a.tags.names = append(a.tags.names, name)
	a.tags.values = append(a.tags.values, name)
}

func (a *metricsAppender) SamplesAppender() (SamplesAppender, error) {
	// Sort tags
	sort.Sort(a.tags)

	// Encode tags and compute a temporary (unowned) ID
	a.tagEncoder.Reset()
	if err := a.tagEncoder.Encode(a.tags); err != nil {
		return nil, err
	}
	data, ok := a.tagEncoder.Data()
	if !ok {
		return nil, fmt.Errorf("unable to encode tags: names=%v, values=%v",
			a.tags.names, a.tags.values)
	}

	unownedID := data.Bytes()

	// Match policies and rollups and build samples appender
	id := a.encodedTagsIteratorPool.Get()
	id.Reset(unownedID)
	now := time.Now()
	fromNanos, toNanos := now.Add(-1*a.clockOpts.MaxNegativeSkew()).UnixNano(),
		now.Add(1*a.clockOpts.MaxPositiveSkew()).UnixNano()
	matchResult := a.matcher.ForwardMatch(id, fromNanos, toNanos)
	id.Close()

	policies := matchResult.MappingsAt(now.UnixNano())

	a.multiSamplesAppender.reset()
	a.multiSamplesAppender.addSamplesAppender(samplesAppender{
		agg:       a.agg,
		unownedID: unownedID,
		policies:  policies,
	})

	numRollups := matchResult.NumRollups()
	for i := 0; i < numRollups; i++ {
		rollup, ok := matchResult.RollupsAt(i, now.UnixNano())
		if !ok {
			continue
		}

		a.multiSamplesAppender.addSamplesAppender(samplesAppender{
			agg:       a.agg,
			unownedID: rollup.ID,
			policies:  rollup.PoliciesList,
		})
	}

	return a.multiSamplesAppender, nil
}

func (a *metricsAppender) Reset() {
	a.tags.names = a.tags.names[:0]
	a.tags.values = a.tags.values[:0]
}

func (a *metricsAppender) Finalize() {
	a.tagEncoder.Finalize()
	a.tagEncoder = nil
}

type samplesAppender struct {
	agg       aggregator.Aggregator
	unownedID []byte
	policies  policy.PoliciesList
}

func (a samplesAppender) AppendCounterSample(value int64) error {
	sample := unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		OwnsID:     false,
		ID:         a.unownedID,
		CounterVal: value,
	}
	return a.agg.AddMetricWithPoliciesList(sample, a.policies)
}

func (a samplesAppender) AppendGaugeSample(value float64) error {
	sample := unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		OwnsID:   false,
		ID:       a.unownedID,
		GaugeVal: value,
	}
	return a.agg.AddMetricWithPoliciesList(sample, a.policies)
}

// Ensure multiSamplesAppender implements SamplesAppender
var _ SamplesAppender = (*multiSamplesAppender)(nil)

type multiSamplesAppender struct {
	appenders []samplesAppender
}

func newMultiSamplesAppender() *multiSamplesAppender {
	return &multiSamplesAppender{}
}

func (a *multiSamplesAppender) reset() {
	var zeroedSamplesAppender samplesAppender
	for i := range a.appenders {
		a.appenders[i] = zeroedSamplesAppender
	}
	a.appenders = a.appenders[:0]
}

func (a *multiSamplesAppender) addSamplesAppender(v samplesAppender) {
	a.appenders = append(a.appenders, v)
}

func (a *multiSamplesAppender) AppendCounterSample(value int64) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendCounterSample(value))
	}
	return multiErr.FinalError()
}

func (a *multiSamplesAppender) AppendGaugeSample(value float64) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendGaugeSample(value))
	}
	return multiErr.FinalError()
}

type tags struct {
	names    []string
	values   []string
	idx      int
	nameBuf  []byte
	valueBuf []byte
}

var _ ident.TagIterator = &tags{}
var _ sort.Interface = &tags{}

func newTags() *tags {
	return &tags{
		names:  make([]string, 0, initAllocTagsSliceCapacity),
		values: make([]string, 0, initAllocTagsSliceCapacity),
		idx:    -1,
	}
}

func (t *tags) Len() int {
	return len(t.names)
}

func (t *tags) Swap(i, j int) {
	t.names[i], t.names[j] = t.names[j], t.names[i]
	t.values[i], t.values[j] = t.values[j], t.values[i]
}

func (t *tags) Less(i, j int) bool {
	return t.names[i] < t.names[j]
}

func (t *tags) Next() bool {
	return t.idx+1 < len(t.names)
}

func (t *tags) Current() ident.Tag {
	t.nameBuf = append(t.nameBuf[:0], t.names[t.idx]...)
	t.valueBuf = append(t.valueBuf[:0], t.values[t.idx]...)
	return ident.Tag{
		Name:  ident.BytesID(t.nameBuf),
		Value: ident.BytesID(t.valueBuf),
	}
}

func (t *tags) Err() error {
	return nil
}

func (t *tags) Close() {

}

func (t *tags) Remaining() int {
	return t.idx + 1 - (len(t.names) - 1)
}

func (t *tags) Duplicate() ident.TagIterator {
	return &tags{idx: -1, names: t.names, values: t.values}
}
