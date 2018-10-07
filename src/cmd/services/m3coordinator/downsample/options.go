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

	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	"github.com/m3db/m3aggregator/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	placementservice "github.com/m3db/m3cluster/placement/service"
	placementstorage "github.com/m3db/m3cluster/placement/storage"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/matcher/cache"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
)

const (
	instanceID                     = "downsampler_local"
	placementKVKey                 = "/placement"
	replicationFactor              = 1
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

// DownsamplerOptions is a set of required downsampler options.
type DownsamplerOptions struct {
	Storage                 storage.Storage
	StorageFlushConcurrency int
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
	aggregator             aggregator.Aggregator
	defaultStagedMetadatas []metadata.StagedMetadatas
	clockOpts              clock.Options
	matcher                matcher.Matcher
	pools                  aggPools
}

func (o DownsamplerOptions) newAggregator() (agg, error) {
	// Validate options first.
	if err := o.validate(); err != nil {
		return agg{}, err
	}

	var (
		storageFlushConcurrency = defaultStorageFlushConcurrency
		rulesStore              = o.RulesKVStore
		clockOpts               = o.ClockOptions
		instrumentOpts          = o.InstrumentOptions
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

	// Use default aggregation types, in future we can provide more configurability
	var defaultAggregationTypes aggregation.TypesConfiguration
	aggTypeOpts, err := defaultAggregationTypes.NewOptions(instrumentOpts)
	if err != nil {
		return agg{}, err
	}

	matcher, err := o.newAggregatorMatcher(clockOpts, instrumentOpts,
		ruleSetOpts, rulesStore)
	if err != nil {
		return agg{}, err
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
		SetAggregationTypesOptions(aggTypeOpts).
		SetMetricPrefix(nil).
		SetCounterPrefix(nil).
		SetGaugePrefix(nil).
		SetTimerPrefix(nil).
		SetAdminClient(adminAggClient).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetFlushManager(flushManager).
		SetFlushHandler(flushHandler)

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
	tagEncoderPool          serialize.TagEncoderPool
	tagDecoderPool          serialize.TagDecoderPool
	encodedTagsIteratorPool *xserialize.EncodedTagsIteratorPool
}

func (o DownsamplerOptions) newAggregatorPools() aggPools {
	tagEncoderPool := serialize.NewTagEncoderPool(o.TagEncoderOptions,
		o.TagEncoderPoolOptions)
	tagEncoderPool.Init()

	tagDecoderPool := serialize.NewTagDecoderPool(o.TagDecoderOptions,
		o.TagDecoderPoolOptions)
	tagDecoderPool.Init()

	encodedTagsIteratorPool := xserialize.NewEncodedTagsIteratorPool(tagDecoderPool,
		o.TagDecoderPoolOptions)
	encodedTagsIteratorPool.Init()

	return aggPools{
		tagEncoderPool:          tagEncoderPool,
		tagDecoderPool:          tagDecoderPool,
		encodedTagsIteratorPool: encodedTagsIteratorPool,
	}
}

func (o DownsamplerOptions) newAggregatorRulesOptions(pools aggPools) rules.Options {
	nameTag := defaultMetricNameTagName
	if o.NameTag != "" {
		nameTag = []byte(o.NameTag)
	}

	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := pools.encodedTagsIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey: nameTag,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, pools.encodedTagsIteratorPool,
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
		return isRollupID(tags, pools.encodedTagsIteratorPool)
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
	handler := newDownsamplerFlushHandler(o.Storage, pools.encodedTagsIteratorPool,
		flushWorkers, instrumentOpts)

	return flushManager, handler
}
