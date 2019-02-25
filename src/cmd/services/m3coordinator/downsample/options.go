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
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/client"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	placementservice "github.com/m3db/m3/src/cluster/placement/service"
	placementstorage "github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
)

const (
	instanceID                         = "downsampler_local"
	placementKVKey                     = "/placement"
	replicationFactor                  = 1
	defaultStorageFlushConcurrency     = 20000
	defaultOpenTimeout                 = 10 * time.Second
	minBufferPast                      = 5 * time.Second
	maxBufferPast                      = 10 * time.Minute
	defaultBufferPastTimedMetricFactor = 0.1
	defaultBufferFutureTimedMetric     = time.Minute
)

var (
	numShards                         = runtime.NumCPU()
	defaultBufferForPastTimedMetricFn = func(r time.Duration) time.Duration {
		value := time.Duration(defaultBufferPastTimedMetricFactor * float64(r))

		// Clamp minBufferPast <= value <= maxBufferPast.
		if value < minBufferPast {
			return minBufferPast
		}
		if value > maxBufferPast {
			return maxBufferPast
		}

		return value
	}

	errNoStorage               = errors.New("dynamic downsampling enabled with storage not set")
	errNoClusterClient         = errors.New("dynamic downsampling enabled with cluster client not set")
	errNoRulesStore            = errors.New("dynamic downsampling enabled with rules store not set")
	errNoClockOptions          = errors.New("dynamic downsampling enabled with clock options not set")
	errNoInstrumentOptions     = errors.New("dynamic downsampling enabled with instrument options not set")
	errNoTagEncoderOptions     = errors.New("dynamic downsampling enabled with tag encoder options not set")
	errNoTagDecoderOptions     = errors.New("dynamic downsampling enabled with tag decoder options not set")
	errNoTagEncoderPoolOptions = errors.New("dynamic downsampling enabled with tag encoder pool options not set")
	errNoTagDecoderPoolOptions = errors.New("dynamic downsampling enabled with tag decoder pool options not set")
)

// DownsamplerOptions is a set of required downsampler options.
type DownsamplerOptions struct {
	Storage                 storage.Storage
	StorageFlushConcurrency int
	ClusterClient           clusterclient.Client
	RulesKVStore            kv.Store
	AutoMappingRules        []MappingRule
	NameTag                 string
	ClockOptions            clock.Options
	InstrumentOptions       instrument.Options
	TagEncoderOptions       serialize.TagEncoderOptions
	TagDecoderOptions       serialize.TagDecoderOptions
	TagEncoderPoolOptions   pool.ObjectPoolOptions
	TagDecoderPoolOptions   pool.ObjectPoolOptions
	OpenTimeout             time.Duration
	TagOptions              models.TagOptions
}

// MappingRule is a mapping rule to apply to metrics.
type MappingRule struct {
	Aggregations []aggregation.Type
	Policies     policy.StoragePolicies
}

// StagedMetadatas returns the corresponding staged metadatas for this mapping rule.
func (r MappingRule) StagedMetadatas() (metadata.StagedMetadatas, error) {
	aggID, err := aggregation.CompressTypes(r.Aggregations...)
	if err != nil {
		return nil, err
	}

	return metadata.StagedMetadatas{
		metadata.StagedMetadata{
			Metadata: metadata.Metadata{
				Pipelines: metadata.PipelineMetadatas{
					metadata.PipelineMetadata{
						AggregationID:   aggID,
						StoragePolicies: r.Policies,
					},
				},
			},
		},
	}, nil
}

// Validate validates the dynamic downsampling options.
func (o DownsamplerOptions) validate() error {
	if o.Storage == nil {
		return errNoStorage
	}
	if o.ClusterClient == nil {
		return errNoClusterClient
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

// agg will have one of aggregator or clientRemote set, the
// rest of the fields must not be nil.
type agg struct {
	aggregator   aggregator.Aggregator
	clientRemote client.Client

	defaultStagedMetadatas []metadata.StagedMetadatas
	clockOpts              clock.Options
	matcher                matcher.Matcher
	pools                  aggPools
}

// Configuration configurates a downsampler.
type Configuration struct {
	// RemoteAggregator specifies that downsampling should be not
	// done locally but sent to a remote aggregator.
	RemoteAggregator *RemoteAggregatorConfiguration `yaml:"remoteAggregator"`

	// AggregationTypes configs the aggregation types.
	AggregationTypes *aggregation.TypesConfiguration `yaml:"aggregationTypes"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`
}

// RemoteAggregatorConfiguration specifies a remote aggregator
// to use for downsampling.
type RemoteAggregatorConfiguration struct {
	// Client is the remote aggregator client.
	Client client.Configuration `yaml:"client"`
}

// NewDownsampler returns a new downsampler.
func (cfg Configuration) NewDownsampler(
	opts DownsamplerOptions,
) (Downsampler, error) {
	agg, err := cfg.newAggregator(opts)
	if err != nil {
		return nil, err
	}

	return &downsampler{
		opts: opts,
		agg:  agg,
	}, nil
}

func (cfg Configuration) newAggregator(o DownsamplerOptions) (agg, error) {
	// Validate options first.
	if err := o.validate(); err != nil {
		return agg{}, err
	}

	var (
		storageFlushConcurrency = defaultStorageFlushConcurrency
		rulesStore              = o.RulesKVStore
		clockOpts               = o.ClockOptions
		instrumentOpts          = o.InstrumentOptions
		scope                   = instrumentOpts.MetricsScope()
		openTimeout             = defaultOpenTimeout
		defaultStagedMetadatas  []metadata.StagedMetadatas
	)
	if o.StorageFlushConcurrency > 0 {
		storageFlushConcurrency = o.StorageFlushConcurrency
	}
	if o.OpenTimeout > 0 {
		openTimeout = o.OpenTimeout
	}
	for _, rule := range o.AutoMappingRules {
		metadatas, err := rule.StagedMetadatas()
		if err != nil {
			return agg{}, err
		}
		defaultStagedMetadatas = append(defaultStagedMetadatas, metadatas)
	}

	pools := o.newAggregatorPools()
	ruleSetOpts := o.newAggregatorRulesOptions(pools)

	matcher, err := o.newAggregatorMatcher(clockOpts, instrumentOpts,
		ruleSetOpts, rulesStore)
	if err != nil {
		return agg{}, err
	}

	if remoteAgg := cfg.RemoteAggregator; remoteAgg != nil {
		// If downsampling setup to use a remote aggregator instead of local
		// aggregator, set that up instead.
		client, err := remoteAgg.Client.NewClient(o.ClusterClient, clockOpts,
			instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("remote-aggregator-client")))
		if err != nil {
			err = fmt.Errorf("could not create remote aggregator client: %v", err)
			return agg{}, err
		}

		return agg{
			clientRemote:           client,
			defaultStagedMetadatas: defaultStagedMetadatas,
			matcher:                matcher,
			pools:                  pools,
		}, nil
	}

	aggClient := client.NewClient(client.NewOptions())
	adminAggClient, ok := aggClient.(client.AdminClient)
	if !ok {
		return agg{}, fmt.Errorf(
			"unable to cast %v to AdminClient", reflect.TypeOf(aggClient))
	}

	serviceID := services.NewServiceID().
		SetEnvironment("production").
		SetName("downsampler").
		SetZone("embedded")

	localKVStore := mem.NewStore()

	placementManager, err := o.newAggregatorPlacementManager(serviceID,
		localKVStore)
	if err != nil {
		return agg{}, err
	}

	flushTimesManager := aggregator.NewFlushTimesManager(
		aggregator.NewFlushTimesManagerOptions().
			SetFlushTimesStore(localKVStore))

	electionManager, err := o.newAggregatorElectionManager(serviceID,
		placementManager, flushTimesManager)
	if err != nil {
		return agg{}, err
	}

	flushManager, flushHandler := o.newAggregatorFlushManagerAndHandler(serviceID,
		placementManager, flushTimesManager, electionManager, instrumentOpts,
		storageFlushConcurrency, pools)

	// Finally construct all options
	aggregatorOpts := aggregator.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetMetricPrefix(nil).
		SetCounterPrefix(nil).
		SetGaugePrefix(nil).
		SetTimerPrefix(nil).
		SetAdminClient(adminAggClient).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetFlushManager(flushManager).
		SetFlushHandler(flushHandler).
		SetBufferForPastTimedMetricFn(defaultBufferForPastTimedMetricFn).
		SetBufferForFutureTimedMetric(defaultBufferFutureTimedMetric)

	if cfg.AggregationTypes != nil {
		aggTypeOpts, err := cfg.AggregationTypes.NewOptions(instrumentOpts)
		if err != nil {
			return agg{}, err
		}
		aggregatorOpts = aggregatorOpts.SetAggregationTypesOptions(aggTypeOpts)
	}

	// Set counter elem pool.
	counterElemPoolOpts := cfg.CounterElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("counter-elem-pool")),
	)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.MustNewCounterElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

	// Set timer elem pool.
	timerElemPoolOpts := cfg.TimerElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool")),
	)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.MustNewTimerElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

	// Set gauge elem pool.
	gaugeElemPoolOpts := cfg.GaugeElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool")),
	)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.MustNewGaugeElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

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
		aggregator:             aggregatorInstance,
		defaultStagedMetadatas: defaultStagedMetadatas,
		matcher:                matcher,
		pools:                  pools,
	}, nil
}

type aggPools struct {
	tagEncoderPool         serialize.TagEncoderPool
	tagDecoderPool         serialize.TagDecoderPool
	metricTagsIteratorPool serialize.MetricTagsIteratorPool
}

func (o DownsamplerOptions) newAggregatorPools() aggPools {
	tagEncoderPool := serialize.NewTagEncoderPool(o.TagEncoderOptions,
		o.TagEncoderPoolOptions)
	tagEncoderPool.Init()

	tagDecoderPool := serialize.NewTagDecoderPool(o.TagDecoderOptions,
		o.TagDecoderPoolOptions)
	tagDecoderPool.Init()

	metricTagsIteratorPool := serialize.NewMetricTagsIteratorPool(tagDecoderPool,
		o.TagDecoderPoolOptions)
	metricTagsIteratorPool.Init()

	return aggPools{
		tagEncoderPool:         tagEncoderPool,
		tagDecoderPool:         tagDecoderPool,
		metricTagsIteratorPool: metricTagsIteratorPool,
	}
}

func (o DownsamplerOptions) newAggregatorRulesOptions(pools aggPools) rules.Options {
	nameTag := defaultMetricNameTagName
	if o.NameTag != "" {
		nameTag = []byte(o.NameTag)
	}

	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := pools.metricTagsIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey: nameTag,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, pools.metricTagsIteratorPool,
				nameTag)
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
		return isRollupID(tags, pools.metricTagsIteratorPool)
	}

	newRollupIDProviderPool := newRollupIDProviderPool(pools.tagEncoderPool,
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

	return rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOpts).
		SetNewRollupIDFn(newRollupIDFn).
		SetIsRollupIDFn(isRollupIDFn)
}

func (o DownsamplerOptions) newAggregatorMatcher(
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	ruleSetOpts rules.Options,
	rulesStore kv.Store,
) (matcher.Matcher, error) {
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

	return matcher.NewMatcher(cache, opts)
}

func (o DownsamplerOptions) newAggregatorPlacementManager(
	serviceID services.ServiceID,
	localKVStore kv.Store,
) (aggregator.PlacementManager, error) {
	instance := placement.NewInstance().
		SetID(instanceID).
		SetWeight(1).
		SetEndpoint(instanceID)

	placementOpts := placement.NewOptions().
		SetIsStaged(true).
		SetShardStateMode(placement.StableShardStateOnly)

	placementSvc := placementservice.NewPlacementService(
		placementstorage.NewPlacementStorage(localKVStore, placementKVKey, placementOpts),
		placementOpts)

	_, err := placementSvc.BuildInitialPlacement([]placement.Instance{instance}, numShards,
		replicationFactor)
	if err != nil {
		return nil, err
	}

	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKVKey).
		SetStagedPlacementStore(localKVStore)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)

	return aggregator.NewPlacementManager(placementManagerOpts), nil
}

func (o DownsamplerOptions) newAggregatorElectionManager(
	serviceID services.ServiceID,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
) (aggregator.ElectionManager, error) {
	leaderValue := instanceID
	campaignOpts, err := services.NewCampaignOptions()
	if err != nil {
		return nil, err
	}

	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)

	leaderService := newLocalLeaderService(serviceID)

	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager)

	return aggregator.NewElectionManager(electionManagerOpts), nil
}

func (o DownsamplerOptions) newAggregatorFlushManagerAndHandler(
	serviceID services.ServiceID,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
	electionManager aggregator.ElectionManager,
	instrumentOpts instrument.Options,
	storageFlushConcurrency int,
	pools aggPools,
) (aggregator.FlushManager, handler.Handler) {
	flushManagerOpts := aggregator.NewFlushManagerOptions().
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetJitterEnabled(false)
	flushManager := aggregator.NewFlushManager(flushManagerOpts)

	flushWorkers := xsync.NewWorkerPool(storageFlushConcurrency)
	flushWorkers.Init()
	handler := newDownsamplerFlushHandler(o.Storage, pools.metricTagsIteratorPool,
		flushWorkers, o.TagOptions, instrumentOpts)

	return flushManager, handler
}
