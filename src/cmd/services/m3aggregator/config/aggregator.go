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
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"strings"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	aggruntime "github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/config/hostid"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	m3sync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
)

var (
	errNoKVClientConfiguration = errors.New("no kv client configuration")
	errEmptyJitterBucketList   = errors.New("empty jitter bucket list")

	defaultNumPassThroughWriters = 16
)

// AggregatorConfiguration contains aggregator configuration.
type AggregatorConfiguration struct {
	// HostID is the local host ID configuration.
	HostID *hostid.Configuration `yaml:"hostID"`

	// InstanceID is the instance ID configuration.
	InstanceID InstanceIDConfiguration `yaml:"instanceID"`

	// AggregationTypes configs the aggregation types.
	AggregationTypes aggregation.TypesConfiguration `yaml:"aggregationTypes"`

	// Common metric prefix.
	MetricPrefix *string `yaml:"metricPrefix"`

	// Counter metric prefix.
	CounterPrefix *string `yaml:"counterPrefix"`

	// Timer metric prefix.
	TimerPrefix *string `yaml:"timerPrefix"`

	// Gauge metric prefix.
	GaugePrefix *string `yaml:"gaugePrefix"`

	// Stream configuration for computing quantiles.
	Stream streamConfiguration `yaml:"stream"`

	// Client configuration.
	Client aggclient.Configuration `yaml:"client"`

	// Placement manager.
	PlacementManager placementManagerConfiguration `yaml:"placementManager"`

	// Hash type used for sharding.
	HashType *sharding.HashType `yaml:"hashType"`

	// Amount of time we buffer writes before shard cutover.
	BufferDurationBeforeShardCutover time.Duration `yaml:"bufferDurationBeforeShardCutover"`

	// Amount of time we buffer writes after shard cutoff.
	BufferDurationAfterShardCutoff time.Duration `yaml:"bufferDurationAfterShardCutoff"`

	// Amount of time we buffer timed metrics in the past.
	BufferDurationForPastTimedMetric time.Duration `yaml:"bufferDurationForPastTimedMetric"`

	// Amount of time we buffer timed metrics in the future.
	BufferDurationForFutureTimedMetric time.Duration `yaml:"bufferDurationForFutureTimedMetric"`

	// Resign timeout.
	ResignTimeout time.Duration `yaml:"resignTimeout"`

	// Flush times manager.
	FlushTimesManager flushTimesManagerConfiguration `yaml:"flushTimesManager"`

	// Election manager.
	ElectionManager electionManagerConfiguration `yaml:"electionManager"`

	// Flush manager.
	FlushManager flushManagerConfiguration `yaml:"flushManager"`

	// Flushing handler configuration.
	Flush handler.FlushHandlerConfiguration `yaml:"flush"`

	// PassThrough m3msg topic name.
	PassThroughTopicName *string `yaml:"passThroughTopicName"`

	// The number of passthrough writers
	NumPassThroughWriters *int `yaml:"numPassThroughWriters"`

	// Forwarding configuration.
	Forwarding forwardingConfiguration `yaml:"forwarding"`

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

	// Maximum number of cached source sets.
	MaxNumCachedSourceSets *int `yaml:"maxNumCachedSourceSets"`

	// Whether to discard NaN aggregated values.
	DiscardNaNAggregatedValues *bool `yaml:"discardNaNAggregatedValues"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`

	// Pool of entries.
	EntryPool pool.ObjectPoolConfiguration `yaml:"entryPool"`
}

// InstanceIDType is the instance ID type that defines how the
// instance ID is constructed, which is then used to lookup the
// aggregator instance in the placement.
type InstanceIDType uint

const (
	// HostIDPortInstanceIDType specifies to use the host ID
	// concatenated with the port to be used for lookup
	// in the placement.
	// NB: this is a legacy instance ID type and is how the instance
	// ID used to be constructed which imposed the strange
	// requirement that the instance ID in the topology used to require
	// the port concat'd with the host ID).
	HostIDPortInstanceIDType InstanceIDType = iota
	// HostIDInstanceIDType specifies to just use the host ID
	// as the instance ID for lookup in the placement.
	HostIDInstanceIDType

	// defaultInstanceIDType must be used as the legacy instance ID
	// since the config needs to be backwards compatible and for those
	// not explicitly specifying the instance ID type it will cause
	// existing placements to not work with latest versions of the aggregator
	// in a backwards compatible fashion.
	defaultInstanceIDType = HostIDPortInstanceIDType
)

func (t InstanceIDType) String() string {
	switch t {
	case HostIDInstanceIDType:
		return "host_id"
	case HostIDPortInstanceIDType:
		return "host_id_port"
	}
	return "unknown"
}

var (
	validInstanceIDTypes = []InstanceIDType{
		HostIDInstanceIDType,
		HostIDPortInstanceIDType,
	}
)

// UnmarshalYAML unmarshals a InstanceIDType into a valid type from string.
func (t *InstanceIDType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = defaultInstanceIDType
		return nil
	}
	strs := make([]string, 0, len(validInstanceIDTypes))
	for _, valid := range validInstanceIDTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf(
		"invalid InstanceIDType '%s' valid types are: %s", str, strings.Join(strs, ", "))
}

// InstanceIDConfiguration is the instance ID configuration.
type InstanceIDConfiguration struct {
	// InstanceIDType specifies how to construct the instance ID
	// that is used for lookup of the aggregator in the placement.
	InstanceIDType InstanceIDType `yaml:"type"`
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

	// Set administrative client.
	// TODO(xichen): client retry threshold likely needs to be low for faster retries.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("client"))
	adminClient, err := c.Client.NewAdminClient(client, clock.NewOptions(), iOpts)
	if err != nil {
		return nil, err
	}
	if err = adminClient.Init(); err != nil {
		return nil, err
	}
	opts = opts.SetAdminClient(adminClient)

	// Set instance ID.
	instanceID, err := c.newInstanceID(address)
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
	if c.BufferDurationForPastTimedMetric != 0 {
		opts = opts.SetBufferForPastTimedMetricFn(bufferForPastTimedMetricFn(c.BufferDurationForPastTimedMetric))
	}
	if c.BufferDurationForFutureTimedMetric != 0 {
		opts = opts.SetBufferForFutureTimedMetric(c.BufferDurationForFutureTimedMetric)
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
	flushManagerOpts, err := c.FlushManager.NewFlushManagerOptions(
		placementManager,
		electionManager,
		flushTimesManager,
		iOpts,
	)
	if err != nil {
		return nil, err
	}
	flushManager := aggregator.NewFlushManager(flushManagerOpts)
	opts = opts.SetFlushManager(flushManager)

	// Set flushing handler.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush-handler"))
	flushHandler, err := c.Flush.NewHandler(client, iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetFlushHandler(flushHandler)

	// Set passthrough writer.
	passThroughScope := scope.SubScope("passthrough-writer")
	iOpts = instrumentOpts.SetMetricsScope(passThroughScope)
	passThroughWriter, err := c.newPassThroughWriter(client, iOpts, passThroughScope, opts.ShardFn())
	if err != nil {
		return nil, err
	}
	opts = opts.SetPassThroughWriter(passThroughWriter)

	// Set max allowed forwarding delay function.
	jitterEnabled := flushManagerOpts.JitterEnabled()
	maxJitterFn := flushManagerOpts.MaxJitterFn()
	maxAllowedForwardingDelayFn := c.Forwarding.MaxAllowedForwardingDelayFn(jitterEnabled, maxJitterFn)
	opts = opts.SetMaxAllowedForwardingDelayFn(maxAllowedForwardingDelayFn)

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
	opts = opts.SetDefaultStoragePolicies(storagePolicies)

	// Set cached source sets options.
	if c.MaxNumCachedSourceSets != nil {
		opts = opts.SetMaxNumCachedSourceSets(*c.MaxNumCachedSourceSets)
	}

	// Set whether to discard NaN aggregated values.
	if c.DiscardNaNAggregatedValues != nil {
		opts = opts.SetDiscardNaNAggregatedValues(*c.DiscardNaNAggregatedValues)
	}

	// Set counter elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("counter-elem-pool"))
	counterElemPoolOpts := c.CounterElemPool.NewObjectPoolOptions(iOpts)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	opts = opts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.MustNewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, opts)
	})

	// Set timer elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool"))
	timerElemPoolOpts := c.TimerElemPool.NewObjectPoolOptions(iOpts)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	opts = opts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.MustNewTimerElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, opts)
	})

	// Set gauge elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool"))
	gaugeElemPoolOpts := c.GaugeElemPool.NewObjectPoolOptions(iOpts)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	opts = opts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.MustNewGaugeElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, opts)
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

func (c *AggregatorConfiguration) newInstanceID(address string) (string, error) {
	var (
		hostIDValue string
		err         error
	)
	if c.HostID != nil {
		hostIDValue, err = c.HostID.Resolve()
	} else {
		hostIDValue, err = os.Hostname()
	}
	if err != nil {
		return "", fmt.Errorf("error determining host ID: %v", err)
	}

	switch c.InstanceID.InstanceIDType {
	case HostIDInstanceIDType:
		return hostIDValue, nil
	case HostIDPortInstanceIDType:
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", fmt.Errorf("error parsing server address %s: %v", address, err)
		}
		return net.JoinHostPort(hostIDValue, port), nil
	default:
		return "", fmt.Errorf("unknown instance ID type: value=%d, str=%s",
			c.InstanceID.InstanceIDType, c.InstanceID.InstanceIDType.String())
	}
}

func bufferForPastTimedMetricFn(buffer time.Duration) aggregator.BufferForPastTimedMetricFn {
	return func(resolution time.Duration) time.Duration {
		return buffer + resolution
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

type forwardingConfiguration struct {
	// MaxSingleDelay is the maximum delay for a single forward step.
	MaxSingleDelay time.Duration `yaml:"maxSingleDelay"`
}

func (c forwardingConfiguration) MaxAllowedForwardingDelayFn(
	jitterEnabled bool,
	maxJitterFn aggregator.FlushJitterFn,
) aggregator.MaxAllowedForwardingDelayFn {
	return func(resolution time.Duration, numForwardedTimes int) time.Duration {
		// If jittering is enabled, we use max jitter fn to determine the initial jitter.
		// Otherwise, flushing may start at any point within a resolution interval so we
		// assume the full resolution interval may be used for initial jittering.
		initialJitter := resolution
		if jitterEnabled {
			initialJitter = maxJitterFn(resolution)
		}
		return initialJitter + c.MaxSingleDelay*time.Duration(numForwardedTimes)
	}
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

func (c flushManagerConfiguration) NewFlushManagerOptions(
	placementManager aggregator.PlacementManager,
	electionManager aggregator.ElectionManager,
	flushTimesManager aggregator.FlushTimesManager,
	instrumentOpts instrument.Options,
) (aggregator.FlushManagerOptions, error) {
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
		runtimeCPU := float64(runtime.NumCPU())
		numWorkers := c.NumWorkersPerCPU * runtimeCPU
		workerPoolSize := int(math.Ceil(numWorkers))
		if workerPoolSize < 1 {
			workerPoolSize = 1
		}
		workerPool := m3sync.NewWorkerPool(workerPoolSize)
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
	return opts, nil
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
	str *string,
	fn metricPrefixSetter,
) aggregator.Options {
	if str == nil {
		return opts
	}
	return fn([]byte(*str))
}

// passThroughWriter writes passthrough metrics to backends.
type passThroughWriter struct {
	numShards int
	writers   []writer.Writer
	locks     []sync.Mutex
	shardFn   sharding.ShardFn
}

func (c *AggregatorConfiguration) newPassThroughWriter(
	cs client.Client,
	iOpts instrument.Options,
	scope tally.Scope,
	shardFn sharding.ShardFn,
) (writer.Writer, error) {
	// This is a temporary change to use a separate m3msg topic for pass-through metrics during the migration.
	for _, handler := range c.Flush.Handlers {
		if handler.DynamicBackend != nil && c.PassThroughTopicName != nil {
			handler.DynamicBackend.Producer.Writer.TopicName = *c.PassThroughTopicName
		}
	}

	count := defaultNumPassThroughWriters
	if c.NumPassThroughWriters != nil {
		count = *c.NumPassThroughWriters
	}

	writers := make([]writer.Writer, 0, count)
	for i := 0; i < count; i++ {
		handler, err := c.Flush.NewHandler(cs, iOpts)
		if err != nil {
			return nil, err
		}
		writer, err := handler.NewWriter(scope)
		if err != nil {
			return nil, err
		}
		writers = append(writers, writer)
	}

	return &passThroughWriter{
		numShards: count,
		writers:   writers,
		locks:     make([]sync.Mutex, count),
		shardFn:   shardFn,
	}, nil
}

func (p *passThroughWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	shardID := p.shardFn([]byte(mp.ChunkedID.String()), uint32(p.numShards))
	p.locks[shardID].Lock()
	defer p.locks[shardID].Unlock()
	return p.writers[shardID].Write(mp)
}

func (p *passThroughWriter) Flush() error {
	errStr := "failed to flush passthrough writer"
	failed := false
	for i := 0; i < p.numShards; i++ {
		p.locks[i].Lock()
		if err := p.writers[i].Flush(); err != nil {
			failed = true
			errStr += fmt.Sprintf(". Writer#%d-%v", i, err)
		}
		p.locks[i].Unlock()
	}
	if failed {
		return errors.New(errStr)
	}
	return nil
}

func (p *passThroughWriter) Close() error {
	errStr := "failed to close passthrough writer"
	failed := false
	for i := 0; i < p.numShards; i++ {
		p.locks[i].Lock()
		if err := p.writers[i].Close(); err != nil {
			failed = true
			errStr += fmt.Sprintf(". Writer#%d-%v", i, err)
		}
		p.locks[i].Unlock()
	}
	if failed {
		return errors.New(errStr)
	}
	return nil
}
