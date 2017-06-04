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
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"

	"github.com/spaolacci/murmur3"
)

const (
	initialBufferSizeGrowthFactor = 2
)

var (
	errUnknownQuantileSuffixFnType   = errors.New("unknown quantile suffix function type")
	errUnknownFlushHandlerType       = errors.New("unknown flush handler type")
	errNoKVClientConfiguration       = errors.New("no kv client configuration")
	errNoForwardHandlerConfiguration = errors.New("no forward flush configuration")
	emptyPolicy                      policy.Policy
)

// AggregatorConfiguration contains aggregator configuration.
type AggregatorConfiguration struct {
	// Common metric prefix.
	MetricPrefix string `yaml:"metricPrefix"`

	// Counter metric prefix.
	CounterPrefix string `yaml:"counterPrefix"`

	// Timer metric prefix.
	TimerPrefix string `yaml:"timerPrefix"`

	// Timer sum metric suffix.
	TimerSumSuffix string `yaml:"timerSumSuffix"`

	// Timer sum square metric suffix.
	TimerSumSqSuffix string `yaml:"timerSumSqSuffix"`

	// Timer sum mean metric suffix.
	TimerMeanSuffix string `yaml:"timerMeanSuffix"`

	// Timer lower metric suffix.
	TimerLowerSuffix string `yaml:"timerLowerSuffix"`

	// Timer upper metric suffix.
	TimerUpperSuffix string `yaml:"timerUpperSuffix"`

	// Timer count metric suffix.
	TimerCountSuffix string `yaml:"timerCountSuffix"`

	// Timer standard deviation metric suffix.
	TimerStdevSuffix string `yaml:"timerStdevSuffix"`

	// Timer median metric suffix.
	TimerMedianSuffix string `yaml:"timerMedianSuffix"`

	// Target quantiles to compute.
	TimerQuantiles []float64 `yaml:"timerQuantiles"`

	// Timer quantile suffix function type.
	TimerQuantileSuffixFnType string `yaml:"timerQuantileSuffixFnType"`

	// Gauge metric suffix.
	GaugePrefix string `yaml:"gaugePrefix"`

	// Stream configuration for computing quantiles.
	Stream streamConfiguration `yaml:"stream"`

	// Client configuration for key value store.
	KVClient kvClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// Placement watcher configuration for watching placement updates.
	PlacementWatcher placementWatcherConfiguration `yaml:"placementWatcher"`

	// Sharding function type.
	ShardFnType *shardFnType `yaml:"shardFnType"`

	// Flush manager.
	FlushManager flushManagerConfiguration `yaml:"flushManager"`

	// Minimum flush interval across all resolutions.
	MinFlushInterval time.Duration `yaml:"minFlushInterval"`

	// Maximum flush size in bytes.
	MaxFlushSize int `yaml:"maxFlushSize"`

	// Flushing handler configuration.
	Flush flushHandlerConfiguration `yaml:"flush"`

	// EntryTTL determines how long an entry remains alive before it may be expired due to inactivity.
	EntryTTL time.Duration `yaml:"entryTTL"`

	// EntryCheckInterval determines how often entries are checked for expiration.
	EntryCheckInterval time.Duration `yaml:"entryCheckInterval"`

	// EntryCheckBatchPercent determines the percentage of entries checked in a batch.
	EntryCheckBatchPercent float64 `yaml:"entryCheckBatchPercent" validate:"min=0.0,max=1.0"`

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
}

// NewAggregatorOptions creates a new set of aggregator options.
func (c *AggregatorConfiguration) NewAggregatorOptions(
	address string,
	instrumentOpts instrument.Options,
) (aggregator.Options, error) {
	scope := instrumentOpts.MetricsScope()
	opts := aggregator.NewOptions().SetInstrumentOptions(instrumentOpts)

	// Set the prefix and suffix for metrics aggregations.
	opts = setMetricPrefixOrSuffix(opts, c.MetricPrefix, opts.SetMetricPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.CounterPrefix, opts.SetCounterPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerPrefix, opts.SetTimerPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerSumSuffix, opts.SetTimerSumSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerSumSqSuffix, opts.SetTimerSumSqSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerMeanSuffix, opts.SetTimerMeanSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerLowerSuffix, opts.SetTimerLowerSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerUpperSuffix, opts.SetTimerUpperSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerCountSuffix, opts.SetTimerCountSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerStdevSuffix, opts.SetTimerStdevSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerMedianSuffix, opts.SetTimerMedianSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.GaugePrefix, opts.SetGaugePrefix)

	// Set timer quantile suffix function.
	quantileSuffixFn, err := c.parseTimerQuantileSuffixFn()
	if err != nil {
		return nil, err
	}
	opts = opts.SetTimerQuantileSuffixFn(quantileSuffixFn).SetTimerQuantiles(c.TimerQuantiles)

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
	opts = opts.SetInstanceID(instanceID)

	// Create kv client.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("kvClient"))
	client, err := c.KVClient.NewKVClient(iOpts)
	if err != nil {
		return nil, err
	}

	// Set placement watcher options.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("placement-watcher"))
	watcherOpts, err := c.PlacementWatcher.NewPlacementWatcherOptions(iOpts, client)
	if err != nil {
		return nil, err
	}
	opts = opts.SetStagedPlacementWatcherOptions(watcherOpts)

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

	// Set flush manager.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush-manager"))
	flushManager := c.FlushManager.NewFlushManager(iOpts)
	opts = opts.SetFlushManager(flushManager)

	// Set flushing handler.
	if c.MinFlushInterval != 0 {
		opts = opts.SetMinFlushInterval(c.MinFlushInterval)
	}
	if c.MaxFlushSize != 0 {
		opts = opts.SetMaxFlushSize(c.MaxFlushSize)
	}
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("flush"))
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
	counterElemPool.Init(func() *aggregator.CounterElem { return aggregator.NewCounterElem(nil, emptyPolicy, opts) })

	// Set timer elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool"))
	timerElemPoolOpts := c.TimerElemPool.NewObjectPoolOptions(iOpts)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	opts = opts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem { return aggregator.NewTimerElem(nil, emptyPolicy, opts) })

	// Set gauge elem pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool"))
	gaugeElemPoolOpts := c.GaugeElemPool.NewObjectPoolOptions(iOpts)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	opts = opts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem { return aggregator.NewGaugeElem(nil, emptyPolicy, opts) })

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
	bufferedEncoderPoolOpts := c.BufferedEncoderPool.NewObjectPoolOptions(iOpts)
	bufferedEncoderPool := msgpack.NewBufferedEncoderPool(bufferedEncoderPoolOpts)
	opts = opts.SetBufferedEncoderPool(bufferedEncoderPool)
	initialBufferSize := c.MaxFlushSize * initialBufferSizeGrowthFactor
	bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoderSize(bufferedEncoderPool, initialBufferSize)
	})

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return opts, nil
}

// parseTimerQuantileSuffixFn parses the quantile suffix function type.
func (c *AggregatorConfiguration) parseTimerQuantileSuffixFn() (aggregator.QuantileSuffixFn, error) {
	fnType := defaultQuantileSuffixType
	if c.TimerQuantileSuffixFnType != "" {
		fnType = timerQuantileSuffixFnType(c.TimerQuantileSuffixFnType)
	}
	switch fnType {
	case defaultQuantileSuffixType:
		return defaultTimerQuantileSuffixFn, nil
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

type placementWatcherConfiguration struct {
	// Placement kv namespace.
	Namespace string `yaml:"namespace" validate:"nonzero"`

	// Placement key.
	Key string `yaml:"key" validate:"nonzero"`

	// Initial watch timeout.
	InitWatchTimeout time.Duration `yaml:"initWatchTimeout"`
}

func (c *placementWatcherConfiguration) NewPlacementWatcherOptions(
	instrumentOpts instrument.Options,
	client client.Client,
) (services.StagedPlacementWatcherOptions, error) {
	store, err := client.Store(c.Namespace)
	if err != nil {
		return nil, err
	}

	opts := placement.NewStagedPlacementWatcherOptions().
		SetInstrumentOptions(instrumentOpts).
		SetStagedPlacementKey(c.Key).
		SetStagedPlacementStore(store)
	if c.InitWatchTimeout != 0 {
		opts = opts.SetInitWatchTimeout(c.InitWatchTimeout)
	}
	return opts, nil
}

type shardFnType string

// List of supported sharding function types.
const (
	unknownShardFn  shardFnType = "unknown"
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

type flushManagerConfiguration struct {
	// How frequently the flush manager checks for next flush.
	CheckEvery time.Duration `yaml:"checkEvery"`

	// Whether jittering is enabled.
	JitterEnabled *bool `yaml:"jitterEnabled"`

	// Number of workers per CPU.
	NumWorkersPerCPU float64 `yaml:"numWorkersPerCPU" validate:"min=0.0,max=1.0"`
}

func (c flushManagerConfiguration) NewFlushManager(
	instrumentOpts instrument.Options,
) aggregator.FlushManager {
	opts := aggregator.NewFlushManagerOptions().SetInstrumentOptions(instrumentOpts)
	if c.CheckEvery != 0 {
		opts = opts.SetCheckEvery(c.CheckEvery)
	}
	if c.JitterEnabled != nil {
		opts = opts.SetJitterEnabled(*c.JitterEnabled)
	}
	if c.NumWorkersPerCPU != 0 {
		workerPoolSize := int(float64(runtime.NumCPU()) * c.NumWorkersPerCPU)
		opts = opts.SetWorkerPoolSize(workerPoolSize)
	}
	return aggregator.NewFlushManager(opts)
}

// flushHandlerConfiguration contains configuration for flushing metrics.
type flushHandlerConfiguration struct {
	// Flushing handler type.
	Type string `yaml:"type" validate:"regexp=(^blackhole$|^logging$|^forward$)"`

	// Forward handler configuration.
	Forward *forwardHandlerConfiguration `yaml:"forward"`
}

func (c *flushHandlerConfiguration) NewHandler(
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	switch handler.Type(c.Type) {
	case handler.BlackholeHandler:
		return handler.NewBlackholeHandler(), nil
	case handler.LoggingHandler:
		return handler.NewLoggingHandler(instrumentOpts), nil
	case handler.ForwardHandler:
		if c.Forward == nil {
			return nil, errNoForwardHandlerConfiguration
		}
		return c.Forward.NewHandler(instrumentOpts)
	default:
		return nil, errUnknownFlushHandlerType
	}
}

// forwardHandlerConfiguration contains configuration for forward handler.
type forwardHandlerConfiguration struct {
	// Server address list.
	Servers []string `yaml:"servers"`

	// Buffer queue size.
	QueueSize int `yaml:"queueSize"`

	// Connection timeout.
	ConnectTimeout time.Duration `yaml:"connectTimeout"`

	// Connection keep alive.
	ConnectionKeepAlive *bool `yaml:"connectionKeepAlive"`

	// Reconnect retrier.
	ReconnectRetrier xretry.Configuration `yaml:"reconnect"`
}

func (c *forwardHandlerConfiguration) NewHandler(
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	opts := handler.NewForwardHandlerOptions().SetInstrumentOptions(instrumentOpts)

	if c.QueueSize != 0 {
		opts = opts.SetQueueSize(c.QueueSize)
	}
	if c.ConnectTimeout != 0 {
		opts = opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}

	scope := instrumentOpts.MetricsScope().SubScope("reconnect")
	retrier := c.ReconnectRetrier.NewRetrier(scope)
	opts = opts.SetReconnectRetrier(retrier)

	return handler.NewForwardHandler(c.Servers, opts)
}

// timerQuantileSuffixFnType is the timer quantile suffix function type.
type timerQuantileSuffixFnType string

// A list of supported timer quantile suffix function types.
const (
	defaultQuantileSuffixType timerQuantileSuffixFnType = "default"
)

func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	return []byte(fmt.Sprintf(".p%0.0f", quantile*100))
}

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
