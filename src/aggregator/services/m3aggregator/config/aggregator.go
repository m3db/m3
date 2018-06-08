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
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	aggruntime "github.com/m3db/m3aggregator/runtime"
	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
)

var (
	errNoKVClientConfiguration = errors.New("no kv client configuration")
	errEmptyJitterBucketList   = errors.New("empty jitter bucket list")
)

// AggregatorConfiguration contains aggregator configuration.
type AggregatorConfiguration struct {
	// AggregationTypes configs the aggregation types.
	AggregationTypes aggregation.TypesConfiguration `yaml:"aggregationTypes"`

	// Common metric prefix.
	MetricPrefix string `yaml:"metricPrefix"`

	// Counter metric prefix.
	CounterPrefix string `yaml:"counterPrefix"`

	// Timer metric prefix.
	TimerPrefix string `yaml:"timerPrefix"`

	// Gauge metric prefix.
	GaugePrefix string `yaml:"gaugePrefix"`

	// Stream configuration for computing quantiles.
	Stream streamConfiguration `yaml:"stream"`

	// Placement manager.
	PlacementManager placementManagerConfiguration `yaml:"placementManager"`

	// Hash type used for sharding.
	HashType *sharding.HashType `yaml:"hashType"`

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

	// Flushing handler configuration.
	Flush handler.FlushHandlerConfiguration `yaml:"flush"`

	// EntryTTL determines how long an entry remains alive before it may be expired due to inactivity.
	EntryTTL time.Duration `yaml:"entryTTL"`

	// EntryCheckInterval determines how often entries are checked for expiration.
	EntryCheckInterval time.Duration `yaml:"entryCheckInterval"`

	// EntryCheckBatchPercent determines the percentage of entries checked in a batch.
	EntryCheckBatchPercent float64 `yaml:"entryCheckBatchPercent" validate:"min=0.0,max=1.0"`

	// MaxTimerBatchSizePerWrite determines the maximum timer batch size for each batched write.
	MaxTimerBatchSizePerWrite int `yaml:"maxTimerBatchSizePerWrite" validate:"min=0"`

	// Default storage policies.
	DefaultStoragePolicies []policy.StoragePolicy `yaml:"defaultStoragePolicies" validate:"nonzero"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`

	// Pool of entries.
	EntryPool pool.ObjectPoolConfiguration `yaml:"entryPool"`
}

// NewAggregatorOptions creates a new set of aggregator options.
func (c *AggregatorConfiguration) NewAggregatorOptions(
	address string,
	client client.Client,
	runtimeOptsManager aggruntime.OptionsManager,
	instrumentOpts instrument.Options,
) (aggregator.Options, error) {
	opts := aggregator.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRuntimeOptionsManager(runtimeOptsManager)

	// Set the aggregation types options.
	aggTypesOpts, err := c.AggregationTypes.NewOptions(instrumentOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetAggregationTypesOptions(aggTypesOpts)

	// Set the prefix for metrics aggregations.
	opts = setMetricPrefix(opts, c.MetricPrefix, opts.SetMetricPrefix)
	opts = setMetricPrefix(opts, c.CounterPrefix, opts.SetCounterPrefix)
	opts = setMetricPrefix(opts, c.TimerPrefix, opts.SetTimerPrefix)
	opts = setMetricPrefix(opts, c.GaugePrefix, opts.SetGaugePrefix)

	// Set stream options.
	scope := instrumentOpts.MetricsScope()
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

	// Set placement manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("placement-manager"))
	placementManager, err := c.PlacementManager.NewPlacementManager(client, instanceID, iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetPlacementManager(placementManager)

	// Set sharding function.
	hashType := sharding.DefaultHash
	if c.HashType != nil {
		hashType = *c.HashType
	}
	shardFn, err := hashType.ShardFn()
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
	flushTimesManager, err := c.FlushTimesManager.NewFlushTimesManager(client, iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetFlushTimesManager(flushTimesManager)

	// Set election manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("election-manager"))
	placementNamespace := c.PlacementManager.KVConfig.Namespace
	electionManager, err := c.ElectionManager.NewElectionManager(
		client,
		instanceID,
		placementNamespace,
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

	// Set default storage policies.
	storagePolicies := make([]policy.StoragePolicy, len(c.DefaultStoragePolicies))
	copy(storagePolicies, c.DefaultStoragePolicies)
	// TODO(xichen): sort the storage policies.
	// sort.Sort(policy.ByResolutionAscRetentionDesc(policies))
	opts = opts.SetDefaultStoragePolicies(storagePolicies)

	// Set counter elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("counter-elem-pool"))
	counterElemPoolOpts := c.CounterElemPool.NewObjectPoolOptions(iOpts)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	opts = opts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.MustNewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, opts)
	})

	// Set timer elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool"))
	timerElemPoolOpts := c.TimerElemPool.NewObjectPoolOptions(iOpts)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	opts = opts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.MustNewTimerElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, opts)
	})

	// Set gauge elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool"))
	gaugeElemPoolOpts := c.GaugeElemPool.NewObjectPoolOptions(iOpts)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	opts = opts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.MustNewGaugeElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, opts)
	})

	// Set entry pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("entry-pool"))
	entryPoolOpts := c.EntryPool.NewObjectPoolOptions(iOpts)
	entryPool := aggregator.NewEntryPool(entryPoolOpts)
	runtimeOpts := runtimeOptsManager.RuntimeOptions()
	opts = opts.SetEntryPool(entryPool)
	entryPool.Init(func() *aggregator.Entry { return aggregator.NewEntry(nil, runtimeOpts, opts) })

	return opts, nil
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

type placementManagerConfiguration struct {
	KVConfig         kv.OverrideConfiguration       `yaml:"kvConfig"`
	PlacementWatcher placement.WatcherConfiguration `yaml:"placementWatcher"`
}

func (c placementManagerConfiguration) NewPlacementManager(
	client client.Client,
	instanceID string,
	instrumentOpts instrument.Options,
) (aggregator.PlacementManager, error) {
	kvOpts, err := c.KVConfig.NewOverrideOptions()
	if err != nil {
		return nil, err
	}
	store, err := client.Store(kvOpts)
	if err != nil {
		return nil, err
	}
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("placement-watcher"))
	placementWatcherOpts := c.PlacementWatcher.NewOptions(store, iOpts)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)
	return aggregator.NewPlacementManager(placementManagerOpts), nil
}

type flushTimesManagerConfiguration struct {
	// KV Configuration.
	KVConfig kv.OverrideConfiguration `yaml:"kvConfig"`

	// Flush times key format.
	FlushTimesKeyFmt string `yaml:"flushTimesKeyFmt" validate:"nonzero"`

	// Retrier for persisting flush times.
	FlushTimesPersistRetrier retry.Configuration `yaml:"flushTimesPersistRetrier"`
}

func (c flushTimesManagerConfiguration) NewFlushTimesManager(
	client client.Client,
	instrumentOpts instrument.Options,
) (aggregator.FlushTimesManager, error) {
	kvOpts, err := c.KVConfig.NewOverrideOptions()
	if err != nil {
		return nil, err
	}
	store, err := client.Store(kvOpts)
	if err != nil {
		return nil, err
	}
	scope := instrumentOpts.MetricsScope()
	retrier := c.FlushTimesPersistRetrier.NewRetrier(scope.SubScope("flush-times-persist"))
	flushTimesManagerOpts := aggregator.NewFlushTimesManagerOptions().
		SetInstrumentOptions(instrumentOpts).
		SetFlushTimesKeyFmt(c.FlushTimesKeyFmt).
		SetFlushTimesStore(store).
		SetFlushTimesPersistRetrier(retrier)
	return aggregator.NewFlushTimesManager(flushTimesManagerOpts), nil
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
	placementNamespace string,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
	instrumentOpts instrument.Options,
) (aggregator.ElectionManager, error) {
	electionOpts, err := c.Election.NewElectionOptions()
	if err != nil {
		return nil, err
	}
	serviceID := c.ServiceID.NewServiceID()
	namespaceOpts := services.NewNamespaceOptions().SetPlacementNamespace(placementNamespace)
	serviceOpts := services.NewOverrideOptions().SetNamespaceOptions(namespaceOpts)
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

type metricPrefixSetter func(b []byte) aggregator.Options

func setMetricPrefix(
	opts aggregator.Options,
	str string,
	fn metricPrefixSetter,
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
		return "", fmt.Errorf("error parsing server address %s: %v", address, err)
	}
	return net.JoinHostPort(hostName, port), nil
}
