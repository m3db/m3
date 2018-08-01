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
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
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
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

const (
	initAllocTagsSliceCapacity     = 32
	shardSetID                     = uint32(0)
	instanceID                     = "downsampler_local"
	placementKVKey                 = "/placement"
	aggregationSuffixTag           = "aggregation"
	defaultStorageFlushConcurrency = 20000
	defaultOpenTimeout             = 10 * time.Second
)

var (
	numShards = runtime.NumCPU()

	errNoStorage               = errors.New("dynamic downsampling enabled with storage not set")
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
	NewMetricsAppender() MetricsAppender
}

// MetricsAppender is a metrics appender that can build a samples
// appender, only valid to use with a single caller at a time.
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
	NameTag                 string
	ClockOptions            clock.Options
	InstrumentOptions       instrument.Options
	TagEncoderOptions       serialize.TagEncoderOptions
	TagDecoderOptions       serialize.TagDecoderOptions
	TagEncoderPoolOptions   pool.ObjectPoolOptions
	TagDecoderPoolOptions   pool.ObjectPoolOptions
	OpenTimeout             time.Duration
}

// Validate validates the dynamic downsampling options.
func (o DownsamplerOptions) validate() error {
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

type agg struct {
	aggregator              aggregator.Aggregator
	clockOpts               clock.Options
	matcher                 matcher.Matcher
	tagEncoderPool          serialize.TagEncoderPool
	tagDecoderPool          serialize.TagDecoderPool
	encodedTagsIteratorPool *encodedTagsIteratorPool
}

func (o DownsamplerOptions) newAggregator() (agg, error) {
	// Validate options first.
	if err := o.validate(); err != nil {
		return agg{}, err
	}

	var (
		storageFlushConcurrency = defaultStorageFlushConcurrency
		rulesStore              = o.RulesKVStore
		nameTag                 = defaultMetricNameTagName
		clockOpts               = o.ClockOptions
		instrumentOpts          = o.InstrumentOptions
		openTimeout             = defaultOpenTimeout
	)
	if o.StorageFlushConcurrency > 0 {
		storageFlushConcurrency = o.StorageFlushConcurrency
	}
	if o.NameTag != "" {
		nameTag = []byte(o.NameTag)
	}
	if o.OpenTimeout > 0 {
		openTimeout = o.OpenTimeout
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
		NameTagKey: nameTag,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, sortedTagIteratorPool, nameTag)
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
		return agg{}, err
	}

	ruleSetOpts := rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOptions).
		SetNewRollupIDFn(newRollupIDFn).
		SetIsRollupIDFn(isRollupIDFn)

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
		return agg{}, err
	}

	aggClient := client.NewClient(client.NewOptions())
	adminAggClient, ok := aggClient.(client.AdminClient)
	if !ok {
		return agg{}, fmt.Errorf(
			"unable to cast %v to AdminClient", reflect.TypeOf(aggClient))
	}

	aggregatorOpts := aggregator.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetAggregationTypesOptions(aggTypeOpts).
		SetMetricPrefix(nil).
		SetCounterPrefix(nil).
		SetGaugePrefix(nil).
		SetTimerPrefix(nil).
		SetAdminClient(adminAggClient)

	shardSet := make([]shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).
			SetState(shard.Initializing)
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
		return agg{}, err
	}

	placementStore := mem.NewStore()
	_, err = placementStore.SetIfNotExists(placementKVKey, stagedPlacementProto)
	if err != nil {
		return agg{}, err
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
		return agg{}, err
	}

	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)

	sid := services.NewServiceID().
		SetEnvironment("production").
		SetName("downsampler").
		SetZone("embedded")

	leaderService := newLocalLeaderService(sid)

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
		SetElectionManager(electionManager).
		SetJitterEnabled(false)
	flushManager := aggregator.NewFlushManager(flushManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushManager(flushManager)

	flushWorkers := xsync.NewWorkerPool(storageFlushConcurrency)
	flushWorkers.Init()
	handler := newDownsamplerFlushHandler(o.Storage, sortedTagIteratorPool,
		flushWorkers, instrumentOpts)
	aggregatorOpts = aggregatorOpts.SetFlushHandler(handler)

	aggregatorInstance := aggregator.NewAggregator(aggregatorOpts)
	if err := aggregatorInstance.Open(); err != nil {
		return agg{}, err
	}

	// Wait until the aggregator becomes leader so we don't miss datapoints
	deadline := time.Now().Add(openTimeout)
	for {
		if !time.Now().Before(deadline) {
			return agg{}, fmt.Errorf("aggregator not promoted to leader after: %s",
				openTimeout.String())
		}
		if electionManager.ElectionState() == aggregator.LeaderState {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return agg{
		aggregator:              aggregatorInstance,
		matcher:                 matcher,
		tagEncoderPool:          tagEncoderPool,
		tagDecoderPool:          tagDecoderPool,
		encodedTagsIteratorPool: sortedTagIteratorPool,
	}, nil
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

		expected := iter.NumTags()
		if len(mp.ChunkedID.Suffix) != 0 {
			expected++
		}
		tags := make(models.Tags, expected)
		for iter.Next() {
			name, value := iter.Current()
			tags[string(name)] = string(value)
		}
		if len(mp.ChunkedID.Suffix) != 0 {
			tags[aggregationSuffixTag] = string(mp.ChunkedID.Suffix)
		}

		err := iter.Err()
		iter.Close()
		if err != nil {
			logger.Errorf("downsampler flush error preparing write: %v", err)
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
			logger.Errorf("downsampler flush error failed write: %v", err)
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		w.handler.metrics.flushSuccess.Inc(1)
	})

	return nil
}

func (w *downsamplerFlushHandlerWriter) Flush() error {
	// NB(r): This is a just simply waiting for inflight requests
	// to complete since this flush handler isn't connection based.
	w.wg.Wait()
	return nil
}

func (w *downsamplerFlushHandlerWriter) Close() error {
	// NB(r): This is a no-op since this flush handler isn't connection based.
	return nil
}

type downsampler struct {
	opts DownsamplerOptions
	agg  agg
}

// NewDownsampler returns a new downsampler.
func NewDownsampler(
	opts DownsamplerOptions,
) (Downsampler, error) {
	agg, err := opts.newAggregator()
	if err != nil {
		return nil, err
	}

	return &downsampler{
		opts: opts,
		agg:  agg,
	}, nil
}

func (d *downsampler) NewMetricsAppender() MetricsAppender {
	return newMetricsAppender(metricsAppenderOptions{
		agg:                     d.agg.aggregator,
		clockOpts:               d.agg.clockOpts,
		tagEncoder:              d.agg.tagEncoderPool.Get(),
		matcher:                 d.agg.matcher,
		encodedTagsIteratorPool: d.agg.encodedTagsIteratorPool,
	})
}

func newMetricsAppender(opts metricsAppenderOptions) *metricsAppender {
	return &metricsAppender{
		metricsAppenderOptions: opts,
		tags:                 newTags(),
		multiSamplesAppender: newMultiSamplesAppender(),
	}
}
