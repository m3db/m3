// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	"github.com/m3db/m3cluster/client"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"

	"github.com/spaolacci/murmur3"
)

const (
	initialBufferSizeGrowthFactor = 2
)

var (
	errUnknownQuantileSuffixFnType = errors.New("unknown quantile suffix function type")
	errNoKVClientConfiguration     = errors.New("no kv client configuration")
	errEmptyJitterBucketList       = errors.New("empty jitter bucket list")
)

// AggregatorConfiguration contains aggregator configuration.
// TODO(xichen): consolidate the kv stores.
type AggregatorConfiguration struct {
	// Default aggregation types for counter metrics.
	CounterAggregationTypes []policy.AggregationType `yaml:"counterAggregationTypes" validate:"nonzero"`

	// Default aggregation types for timer metrics.
	TimerAggregationTypes []policy.AggregationType `yaml:"timerAggregationTypes" validate:"nonzero"`

	// Default aggregation types for gauge metrics.
	GaugeAggregationTypes []policy.AggregationType `yaml:"gaugeAggregationTypes" validate:"nonzero"`

	// Common metric prefix.
	MetricPrefix string `yaml:"metricPrefix"`

	// Counter metric prefix.
	CounterPrefix string `yaml:"counterPrefix"`

	// Timer metric prefix.
	TimerPrefix string `yaml:"timerPrefix"`

	// Gauge metric suffix.
	GaugePrefix string `yaml:"gaugePrefix"`

	// Metric suffix for aggregation type last.
	AggregationLastSuffix string `yaml:"aggregationLastSuffix"`

	// Metric suffix for aggregation type sum.
	AggregationSumSuffix string `yaml:"aggregationSumSuffix"`

	// Metric suffix for aggregation type sum square.
	AggregationSumSqSuffix string `yaml:"aggregationSumSqSuffix"`

	// Metric suffix for aggregation type mean.
	AggregationMeanSuffix string `yaml:"aggregationMeanSuffix"`

	// Metric suffix for aggregation type min.
	AggregationMinSuffix string `yaml:"aggregationMinSuffix"`

	// Metric suffix for aggregation type max.
	AggregationMaxSuffix string `yaml:"aggregationMaxSuffix"`

	// Metric suffix for aggregation type count.
	AggregationCountSuffix string `yaml:"aggregationCountSuffix"`

	// Metric suffix for aggregation type standard deviation.
	AggregationStdevSuffix string `yaml:"aggregationStdevSuffix"`

	// Metric suffix for aggregation type median.
	AggregationMedianSuffix string `yaml:"aggregationMedianSuffix"`

	// Timer quantile suffix function type.
	TimerQuantileSuffixFnType string `yaml:"timerQuantileSuffixFnType"`

	// Counter suffix override.
	CounterSuffixOverride map[policy.AggregationType]string `yaml:"counterSuffixOverride"`

	// Timer suffix override.
	TimerSuffixOverride map[policy.AggregationType]string `yaml:"timerSuffixOverride"`

	// Gauge suffix override.
	GaugeSuffixOverride map[policy.AggregationType]string `yaml:"gaugeSuffixOverride"`

	// Stream configuration for computing quantiles.
	Stream streamConfiguration `yaml:"stream"`

	// Client configuration for key value store.
	KVClient kvClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// KV namespace.
	KVNamespace string `yaml:"kvNamespace" validate:"nonzero"`

	// Placement manager.
	PlacementManager placementManagerConfiguration `yaml:"placementManager"`

	// Sharding function type.
	ShardFnType *shardFnType `yaml:"shardFnType"`

	// Amount of time we buffer writes before shard cutover.
	BufferDurationBeforeShardCutover time.Duration `yaml:"bufferDurationBeforeShardCutover"`

	// Amount of time we buffer writes after shard cutoff.
	BufferDurationAfterShardCutoff time.Duration `yaml:"bufferDurationAfterShardCutoff"`

	// Resign timeout.
	ResignTimeout time.Duration `yaml:"resignTimeout"`

	// Flush times manager.
	FlushTimesManager flushTimesManagerConfiguration `yaml:"flushTimesManager"`

	// Election manager.
	ElectionManager electionManagerConfiguration `yaml:"electionManager"`

	// Flush manager.
	FlushManager flushManagerConfiguration `yaml:"flushManager"`

	// Minimum flush interval across all resolutions.
	MinFlushInterval time.Duration `yaml:"minFlushInterval"`

	// Maximum flush size in bytes.
	MaxFlushSize int `yaml:"maxFlushSize"`

	// Flushing handler configuration.
	Flush *handler.FlushHandlerConfiguration `yaml:"flush"`

	// EntryTTL determines how long an entry remains alive before it may be expired due to inactivity.
	EntryTTL time.Duration `yaml:"entryTTL"`

	// EntryCheckInterval determines how often entries are checked for expiration.
	EntryCheckInterval time.Duration `yaml:"entryCheckInterval"`

	// EntryCheckBatchPercent determines the percentage of entries checked in a batch.
	EntryCheckBatchPercent float64 `yaml:"entryCheckBatchPercent" validate:"min=0.0,max=1.0"`

	// MaxTimerBatchSizePerWrite determines the maximum timer batch size for each batched write.
	MaxTimerBatchSizePerWrite int `yaml:"maxTimerBatchSizePerWrite" validate:"min=0"`

	// Default policies.
	DefaultPolicies []policy.Policy `yaml:"defaultPolicies" validate:"nonzero"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`

	// Pool of entries.
	EntryPool pool.ObjectPoolConfiguration `yaml:"entryPool"`

	// Pool of buffered encoders.
	BufferedEncoderPool pool.ObjectPoolConfiguration `yaml:"bufferedEncoderPool"`

	// Pool of aggregation types.
	AggregationTypesPool pool.ObjectPoolConfiguration `yaml:"aggregationTypesPool"`

	// Pool of quantile slices.
	QuantilesPool pool.BucketizedPoolConfiguration `yaml:"quantilesPool"`
}

// NewAggregatorOptions creates a new set of aggregator options.
func (c *AggregatorConfiguration) NewAggregatorOptions(
	address string,
	instrumentOpts instrument.Options,
) (aggregator.Options, error) {
	scope := instrumentOpts.MetricsScope()
	opts := aggregator.NewOptions().SetInstrumentOptions(instrumentOpts)

	opts = opts.SetDefaultCounterAggregationTypes(c.CounterAggregationTypes).
		SetDefaultTimerAggregationTypes(c.TimerAggregationTypes).
		SetDefaultGaugeAggregationTypes(c.GaugeAggregationTypes)

	// Set the prefix and suffix for metrics aggregations.
	opts = setMetricPrefixOrSuffix(opts, c.MetricPrefix, opts.SetMetricPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.CounterPrefix, opts.SetCounterPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerPrefix, opts.SetTimerPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationLastSuffix, opts.SetAggregationLastSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationSumSuffix, opts.SetAggregationSumSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationSumSqSuffix, opts.SetAggregationSumSqSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationMeanSuffix, opts.SetAggregationMeanSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationMinSuffix, opts.SetAggregationMinSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationMaxSuffix, opts.SetAggregationMaxSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationCountSuffix, opts.SetAggregationCountSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationStdevSuffix, opts.SetAggregationStdevSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.AggregationMedianSuffix, opts.SetAggregationMedianSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.GaugePrefix, opts.SetGaugePrefix)

	// Set timer quantiles and quantile suffix function.
	quantileSuffixFn, err := c.parseTimerQuantileSuffixFn(opts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetTimerQuantileSuffixFn(quantileSuffixFn)

	opts = opts.SetCounterSuffixOverride(c.parseSuffixOverride(c.CounterSuffixOverride)).
		SetTimerSuffixOverride(c.parseSuffixOverride(c.TimerSuffixOverride)).
		SetGaugeSuffixOverride(c.parseSuffixOverride(c.GaugeSuffixOverride))

	// Set stream options.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("stream"))
	streamOpts, err := c.Stream.NewStreamOptions(iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetStreamOptions(streamOpts)

	// Set instance id.
	instanceID, err := instanceID(address)
	if err != nil {
		return nil, err
	}

	// Create kv client.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("kvClient"))
	client, err := c.KVClient.NewKVClient(iOpts)
	if err != nil {
		return nil, err
	}
	store, err := client.Store(c.KVNamespace)
	if err != nil {
		return nil, err
	}

	// Set placement manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("placement-manager"))
	placementManager := c.PlacementManager.NewPlacementManager(instanceID, store, iOpts)
	opts = opts.SetPlacementManager(placementManager)

	// Set sharding function.
	shardFnType := defaultShardFn
	if c.ShardFnType != nil {
		shardFnType = *c.ShardFnType
	}
	shardFn, err := shardFnType.ShardFn()
	if err != nil {
		return nil, err
	}
	opts = opts.SetShardFn(shardFn)

	// Set buffer durations for shard cutovers and shard cutoffs.
	if c.BufferDurationBeforeShardCutover != 0 {
		opts = opts.SetBufferDurationBeforeShardCutover(c.BufferDurationBeforeShardCutover)
	}
	if c.BufferDurationAfterShardCutoff != 0 {
		opts = opts.SetBufferDurationAfterShardCutoff(c.BufferDurationAfterShardCutoff)
	}

	// Set resign timeout.
	if c.ResignTimeout != 0 {
		opts = opts.SetResignTimeout(c.ResignTimeout)
	}

	// Set flush times manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush-times-manager"))
	flushTimesManager := c.FlushTimesManager.NewFlushTimesManager(store, iOpts)
	opts = opts.SetFlushTimesManager(flushTimesManager)

	// Set election manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("election-manager"))
	electionManager, err := c.ElectionManager.NewElectionManager(
		client,
		instanceID,
		c.KVNamespace,
		placementManager,
		flushTimesManager,
		iOpts,
	)
	if err != nil {
		return nil, err
	}
	opts = opts.SetElectionManager(electionManager)

	// Set flush manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush-manager"))
	flushManager, err := c.FlushManager.NewFlushManager(
		placementManager,
		electionManager,
		flushTimesManager,
		iOpts,
	)
	if err != nil {
		return nil, err
	}
	opts = opts.SetFlushManager(flushManager)

	// Set flushing handler.
	if c.MinFlushInterval != 0 {
		opts = opts.SetMinFlushInterval(c.MinFlushInterval)
	}
	if c.MaxFlushSize != 0 {
		opts = opts.SetMaxFlushSize(c.MaxFlushSize)
	}
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush-handler"))
	flushHandler, err := c.Flush.NewHandler(iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetFlushHandler(flushHandler)

	// Set entry options.
	if c.EntryTTL != 0 {
		opts = opts.SetEntryTTL(c.EntryTTL)
	}
	if c.EntryCheckInterval != 0 {
		opts = opts.SetEntryCheckInterval(c.EntryCheckInterval)
	}
	if c.EntryCheckBatchPercent != 0.0 {
		opts = opts.SetEntryCheckBatchPercent(c.EntryCheckBatchPercent)
	}
	if c.MaxTimerBatchSizePerWrite != 0 {
		opts = opts.SetMaxTimerBatchSizePerWrite(c.MaxTimerBatchSizePerWrite)
	}

	// Set default policies.
	policies := make([]policy.Policy, len(c.DefaultPolicies))
	copy(policies, c.DefaultPolicies)
	sort.Sort(policy.ByResolutionAsc(policies))
	opts = opts.SetDefaultPolicies(policies)

	// Set counter elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("counter-elem-pool"))
	counterElemPoolOpts := c.CounterElemPool.NewObjectPoolOptions(iOpts)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	opts = opts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.NewCounterElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	})

	// Set timer elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool"))
	timerElemPoolOpts := c.TimerElemPool.NewObjectPoolOptions(iOpts)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	opts = opts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.NewTimerElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	})

	// Set gauge elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool"))
	gaugeElemPoolOpts := c.GaugeElemPool.NewObjectPoolOptions(iOpts)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	opts = opts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.NewGaugeElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	})

	// Set entry pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("entry-pool"))
	entryPoolOpts := c.EntryPool.NewObjectPoolOptions(iOpts)
	entryPool := aggregator.NewEntryPool(entryPoolOpts)
	opts = opts.SetEntryPool(entryPool)
	entryPool.Init(func() *aggregator.Entry { return aggregator.NewEntry(nil, opts) })

	// Set buffered encoder pool.
	// NB(xichen): we preallocate a bit over the maximum flush size as a safety measure
	// because we might write past the max flush size and rewind it during flushing.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("buffered-encoder-pool"))
	bufferedEncoderPoolOpts := msgpack.NewBufferedEncoderPoolOptions().
		SetObjectPoolOptions(c.BufferedEncoderPool.NewObjectPoolOptions(iOpts))
	bufferedEncoderPool := msgpack.NewBufferedEncoderPool(bufferedEncoderPoolOpts)
	opts = opts.SetBufferedEncoderPool(bufferedEncoderPool)
	initialBufferSize := c.MaxFlushSize * initialBufferSizeGrowthFactor
	bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoderSize(bufferedEncoderPool, initialBufferSize)
	})

	// Set aggregation types pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("aggregation-types-pool"))
	aggTypesPoolOpts := c.AggregationTypesPool.NewObjectPoolOptions(iOpts)
	aggTypesPool := policy.NewAggregationTypesPool(aggTypesPoolOpts)
	opts = opts.SetAggregationTypesPool(aggTypesPool)
	aggTypesPool.Init(func() policy.AggregationTypes {
		return make(policy.AggregationTypes, 0, len(policy.ValidAggregationTypes))
	})

	// Set quantiles pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("quantile-pool"))
	quantilesPool := pool.NewFloatsPool(
		c.QuantilesPool.NewBuckets(),
		c.QuantilesPool.NewObjectPoolOptions(iOpts),
	)
	opts = opts.SetQuantilesPool(quantilesPool)
	quantilesPool.Init()

	return opts, nil
}

// parseSuffixOverride parses the suffix override map.
func (c *AggregatorConfiguration) parseSuffixOverride(m map[policy.AggregationType]string) map[policy.AggregationType][]byte {
	res := make(map[policy.AggregationType][]byte, len(m))
	for aggType, s := range m {
		var bytes []byte
		if s != "" {
			// NB(cw) []byte("") is empty with a cap of 8.
			bytes = []byte(s)
		}
		res[aggType] = bytes
	}
	return res
}

// parseTimerQuantileSuffixFn parses the quantile suffix function type.
func (c *AggregatorConfiguration) parseTimerQuantileSuffixFn(opts aggregator.Options) (aggregator.QuantileSuffixFn, error) {
	fnType := defaultQuantileSuffixType
	if c.TimerQuantileSuffixFnType != "" {
		fnType = timerQuantileSuffixFnType(c.TimerQuantileSuffixFnType)
	}
	switch fnType {
	case defaultQuantileSuffixType:
		return opts.TimerQuantileSuffixFn(), nil
	default:
		return nil, errUnknownQuantileSuffixFnType
	}
}

// streamConfiguration contains configuration for quantile-related metric streams.
type streamConfiguration struct {
	// Error epsilon for quantile computation.
	Eps float64 `yaml:"eps"`

	// Initial heap capacity for quantile computation.
	Capacity int `yaml:"capacity"`

	// Insertion and compression frequency.
	InsertAndCompressEvery int `yaml:"insertAndCompressEvery"`

	// Flush frequency.
	FlushEvery int `yaml:"flushEvery"`

	// Pool of streams.
	StreamPool pool.ObjectPoolConfiguration `yaml:"streamPool"`

	// Pool of metric samples.
	SamplePool *pool.ObjectPoolConfiguration `yaml:"samplePool"`

	// Pool of float slices.
	FloatsPool pool.BucketizedPoolConfiguration `yaml:"floatsPool"`
}

func (c *streamConfiguration) NewStreamOptions(instrumentOpts instrument.Options) (cm.Options, error) {
	scope := instrumentOpts.MetricsScope()
	opts := cm.NewOptions().
		SetEps(c.Eps).
		SetCapacity(c.Capacity)

	if c.InsertAndCompressEvery != 0 {
		opts = opts.SetInsertAndCompressEvery(c.InsertAndCompressEvery)
	}
	if c.FlushEvery != 0 {
		opts = opts.SetFlushEvery(c.FlushEvery)
	}

	if c.SamplePool != nil {
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("sample-pool"))
		samplePoolOpts := c.SamplePool.NewObjectPoolOptions(iOpts)
		samplePool := cm.NewSamplePool(samplePoolOpts)
		opts = opts.SetSamplePool(samplePool)
		samplePool.Init()
	}

	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("floats-pool"))
	floatsPoolOpts := c.FloatsPool.NewObjectPoolOptions(iOpts)
	floatsPool := pool.NewFloatsPool(c.FloatsPool.NewBuckets(), floatsPoolOpts)
	opts = opts.SetFloatsPool(floatsPool)
	floatsPool.Init()

	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("stream-pool"))
	streamPoolOpts := c.StreamPool.NewObjectPoolOptions(iOpts)
	streamPool := cm.NewStreamPool(streamPoolOpts)
	opts = opts.SetStreamPool(streamPool)
	streamPool.Init(func() cm.Stream { return cm.NewStream(nil, opts) })

	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

// TODO(xichen): add configuration for in-memory client with pre-populated data
// for different namespaces so we can start up m3aggregator without a real etcd cluster.
type kvClientConfiguration struct {
	Etcd *etcdclient.Configuration `yaml:"etcd"`
}

func (c *kvClientConfiguration) NewKVClient(instrumentOpts instrument.Options) (client.Client, error) {
	if c.Etcd == nil {
		return nil, errNoKVClientConfiguration
	}
	return c.Etcd.NewClient(instrumentOpts)
}

type shardFnType string

// List of supported sharding function types.
const (
	murmur32ShardFn shardFnType = "murmur32"

	defaultShardFn = murmur32ShardFn
)

var (
	validShardFnTypes = []shardFnType{
		murmur32ShardFn,
	}
)

func (t *shardFnType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = defaultShardFn
		return nil
	}
	validTypes := make([]string, 0, len(validShardFnTypes))
	for _, valid := range validShardFnTypes {
		if str == string(valid) {
			*t = valid
			return nil
		}
		validTypes = append(validTypes, string(valid))
	}
	return fmt.Errorf("invalid shrading function type '%s' valid types are: %s",
		str, strings.Join(validTypes, ", "))
}

func (t shardFnType) ShardFn() (aggregator.ShardFn, error) {
	switch t {
	case murmur32ShardFn:
		return func(id []byte, numShards int) uint32 {
			return murmur3.Sum32(id) % uint32(numShards)
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized hash gen type %v", t)
	}
}

type placementManagerConfiguration struct {
	PlacementWatcher placement.WatcherConfiguration `yaml:"placementWatcher"`
}

func (c placementManagerConfiguration) NewPlacementManager(
	instanceID string,
	store kv.Store,
	instrumentOpts instrument.Options,
) aggregator.PlacementManager {
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("placement-watcher"))
	placementWatcherOpts := c.PlacementWatcher.NewOptions(store, iOpts)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)
	return aggregator.NewPlacementManager(placementManagerOpts)
}

type flushTimesManagerConfiguration struct {
	// Flush times key format.
	FlushTimesKeyFmt string `yaml:"flushTimesKeyFmt" validate:"nonzero"`

	// Retrier for persisting flush times.
	FlushTimesPersistRetrier retry.Configuration `yaml:"flushTimesPersistRetrier"`
}

func (c flushTimesManagerConfiguration) NewFlushTimesManager(
	store kv.Store,
	instrumentOpts instrument.Options,
) aggregator.FlushTimesManager {
	scope := instrumentOpts.MetricsScope()
	retrier := c.FlushTimesPersistRetrier.NewRetrier(scope.SubScope("flush-times-persist"))
	flushTimesManagerOpts := aggregator.NewFlushTimesManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetFlushTimesKeyFmt(c.FlushTimesKeyFmt).
		SetFlushTimesStore(store).
		SetFlushTimesPersistRetrier(retrier)
	return aggregator.NewFlushTimesManager(flushTimesManagerOpts)
}

type electionManagerConfiguration struct {
	Election                   electionConfiguration  `yaml:"election"`
	ServiceID                  serviceIDConfiguration `yaml:"serviceID"`
	LeaderValue                string                 `yaml:"leaderValue"`
	ElectionKeyFmt             string                 `yaml:"electionKeyFmt" validate:"nonzero"`
	CampaignRetrier            retry.Configuration    `yaml:"campaignRetrier"`
	ChangeRetrier              retry.Configuration    `yaml:"changeRetrier"`
	ResignRetrier              retry.Configuration    `yaml:"resignRetrier"`
	CampaignStateCheckInterval time.Duration          `yaml:"campaignStateCheckInterval"`
	ShardCutoffCheckOffset     time.Duration          `yaml:"shardCutoffCheckOffset"`
}

func (c electionManagerConfiguration) NewElectionManager(
	client client.Client,
	instanceID string,
	kvNamespace string,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
	instrumentOpts instrument.Options,
) (aggregator.ElectionManager, error) {
	electionOpts, err := c.Election.NewElectionOptions()
	if err != nil {
		return nil, err
	}
	serviceID := c.ServiceID.NewServiceID()
	namespaceOpts := services.NewNamespaceOptions().SetPlacementNamespace(kvNamespace)
	serviceOpts := services.NewOptions().SetNamespaceOptions(namespaceOpts)
	svcs, err := client.Services(serviceOpts)
	if err != nil {
		return nil, err
	}
	leaderService, err := svcs.LeaderService(serviceID, electionOpts)
	if err != nil {
		return nil, err
	}
	campaignOpts, err := services.NewCampaignOptions()
	if err != nil {
		return nil, err
	}
	leaderValue := instanceID
	if c.LeaderValue != "" {
		leaderValue = c.LeaderValue
	}
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	scope := instrumentOpts.MetricsScope()
	campaignRetryOpts := c.CampaignRetrier.NewOptions(scope.SubScope("campaign"))
	changeRetryOpts := c.ChangeRetrier.NewOptions(scope.SubScope("change"))
	resignRetryOpts := c.ResignRetrier.NewOptions(scope.SubScope("resign"))
	opts := aggregator.NewElectionManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetElectionOptions(electionOpts).
		SetCampaignOptions(campaignOpts).
		SetCampaignRetryOptions(campaignRetryOpts).
		SetChangeRetryOptions(changeRetryOpts).
		SetResignRetryOptions(resignRetryOpts).
		SetElectionKeyFmt(c.ElectionKeyFmt).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager)
	if c.CampaignStateCheckInterval != 0 {
		opts = opts.SetCampaignStateCheckInterval(c.CampaignStateCheckInterval)
	}
	if c.ShardCutoffCheckOffset != 0 {
		opts = opts.SetShardCutoffCheckOffset(c.ShardCutoffCheckOffset)
	}
	electionManager := aggregator.NewElectionManager(opts)
	return electionManager, nil
}

type electionConfiguration struct {
	LeaderTimeout time.Duration `yaml:"leaderTimeout"`
	ResignTimeout time.Duration `yaml:"resignTimeout"`
	TTLSeconds    int           `yaml:"ttlSeconds"`
}

func (c electionConfiguration) NewElectionOptions() (services.ElectionOptions, error) {
	opts := services.NewElectionOptions()
	if c.LeaderTimeout != 0 {
		opts = opts.SetLeaderTimeout(c.LeaderTimeout)
	}
	if c.ResignTimeout != 0 {
		opts = opts.SetResignTimeout(c.ResignTimeout)
	}
	if c.TTLSeconds != 0 {
		opts = opts.SetTTLSecs(c.TTLSeconds)
	}
	return opts, nil
}

// TODO: move this to m3cluster.
type serviceIDConfiguration struct {
	Name        string `yaml:"name"`
	Environment string `yaml:"environment"`
	Zone        string `yaml:"zone"`
}

func (c serviceIDConfiguration) NewServiceID() services.ServiceID {
	sid := services.NewServiceID()
	if c.Name != "" {
		sid = sid.SetName(c.Name)
	}
	if c.Environment != "" {
		sid = sid.SetEnvironment(c.Environment)
	}
	if c.Zone != "" {
		sid = sid.SetZone(c.Zone)
	}
	return sid
}

type flushManagerConfiguration struct {
	// How frequently the flush manager checks for next flush.
	CheckEvery time.Duration `yaml:"checkEvery"`

	// Whether jittering is enabled.
	JitterEnabled *bool `yaml:"jitterEnabled"`

	// Buckets for determining max jitter amounts.
	MaxJitters []jitterBucket `yaml:"maxJitters"`

	// Number of workers per CPU.
	NumWorkersPerCPU float64 `yaml:"numWorkersPerCPU" validate:"min=0.0,max=1.0"`

	// How frequently the flush times are persisted.
	FlushTimesPersistEvery time.Duration `yaml:"flushTimesPersistEvery"`

	// Maximum buffer size.
	MaxBufferSize time.Duration `yaml:"maxBufferSize"`

	// Window size for a forced flush.
	ForcedFlushWindowSize time.Duration `yaml:"forcedFlushWindowSize"`
}

func (c flushManagerConfiguration) NewFlushManager(
	placementManager aggregator.PlacementManager,
	electionManager aggregator.ElectionManager,
	flushTimesManager aggregator.FlushTimesManager,
	instrumentOpts instrument.Options,
) (aggregator.FlushManager, error) {
	opts := aggregator.NewFlushManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetPlacementManager(placementManager).
		SetElectionManager(electionManager).
		SetFlushTimesManager(flushTimesManager)
	if c.CheckEvery != 0 {
		opts = opts.SetCheckEvery(c.CheckEvery)
	}
	if c.JitterEnabled != nil {
		opts = opts.SetJitterEnabled(*c.JitterEnabled)
	}
	if c.MaxJitters != nil {
		maxJitterFn, err := jitterBuckets(c.MaxJitters).NewMaxJitterFn()
		if err != nil {
			return nil, err
		}
		opts = opts.SetMaxJitterFn(maxJitterFn)
	}
	if c.NumWorkersPerCPU != 0 {
		workerPoolSize := int(float64(runtime.NumCPU()) * c.NumWorkersPerCPU)
		workerPool := sync.NewWorkerPool(workerPoolSize)
		workerPool.Init()
		opts = opts.SetWorkerPool(workerPool)
	}
	if c.FlushTimesPersistEvery != 0 {
		opts = opts.SetFlushTimesPersistEvery(c.FlushTimesPersistEvery)
	}
	if c.MaxBufferSize != 0 {
		opts = opts.SetMaxBufferSize(c.MaxBufferSize)
	}
	if c.ForcedFlushWindowSize != 0 {
		opts = opts.SetForcedFlushWindowSize(c.ForcedFlushWindowSize)
	}
	return aggregator.NewFlushManager(opts), nil
}

// jitterBucket determines the max jitter percent for lists whose flush
// intervals are no more than the bucket flush interval.
type jitterBucket struct {
	FlushInterval    time.Duration `yaml:"flushInterval" validate:"nonzero"`
	MaxJitterPercent float64       `yaml:"maxJitterPercent" validate:"min=0.0,max=1.0"`
}

type jitterBuckets []jitterBucket

func (buckets jitterBuckets) NewMaxJitterFn() (aggregator.FlushJitterFn, error) {
	numBuckets := len(buckets)
	if numBuckets == 0 {
		return nil, errEmptyJitterBucketList
	}
	res := make([]jitterBucket, numBuckets)
	copy(res, buckets)
	sort.Sort(jitterBucketsByIntervalAscending(res))

	return func(interval time.Duration) time.Duration {
		idx := sort.Search(numBuckets, func(i int) bool {
			return res[i].FlushInterval >= interval
		})
		if idx == numBuckets {
			idx--
		}
		return time.Duration(res[idx].MaxJitterPercent * float64(interval))
	}, nil
}

type jitterBucketsByIntervalAscending []jitterBucket

func (b jitterBucketsByIntervalAscending) Len() int      { return len(b) }
func (b jitterBucketsByIntervalAscending) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func (b jitterBucketsByIntervalAscending) Less(i, j int) bool {
	return b[i].FlushInterval < b[j].FlushInterval
}

// timerQuantileSuffixFnType is the timer quantile suffix function type.
type timerQuantileSuffixFnType string

// A list of supported timer quantile suffix function types.
const (
	defaultQuantileSuffixType timerQuantileSuffixFnType = "default"
)

type metricPrefixOrSuffixSetter func(prefixOrSuffix []byte) aggregator.Options

func setMetricPrefixOrSuffix(
	opts aggregator.Options,
	str string,
	fn metricPrefixOrSuffixSetter,
) aggregator.Options {
	if str == "" {
		return opts
	}
	return fn([]byte(str))
}

func instanceID(address string) (string, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("error determining host name: %v", err)
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("error parse msgpack server address %s: %v", address, err)
	}
	return net.JoinHostPort(hostName, port), nil
}
