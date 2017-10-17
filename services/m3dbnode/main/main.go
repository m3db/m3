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

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	hjcluster "github.com/m3db/m3db/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3db/network/server/httpjson/node"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	ttnode "github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	m3dbruntime "github.com/m3db/m3db/runtime"
	bconfig "github.com/m3db/m3db/services/m3dbnode/config"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/cluster"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	m3dbxio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
)

type calculationType string

const (
	calculationTypeFixed       calculationType = "fixed"
	calculationTypePerCPU      calculationType = "percpu"
	bootstrapConfigInitTimeout time.Duration   = 10 * time.Second
)

type hostIDResolver string

const (
	hostnameHostIDResolver    hostIDResolver = "hostname"
	configHostIDResolver      hostIDResolver = "config"
	environmentHostIDResolver hostIDResolver = "environment"

	defaultHostIDResolver = hostnameHostIDResolver
)

// TODO(r): move to its own pacakage
const (
	KVStoreBootstrapperKey             = "m3db.node.bootstrapper"
	KVStoreNamespacesKey               = "m3db.node.namespaces"
	KVStoreClusterNewSeriesInsertLimit = "m3db.node.cluster-new-series-insert-limit"
)

var (
	timeZero time.Time

	errNoShards = errors.New("found service instance with no shards")
)

type hostIDConfiguration struct {
	// Resolver is the resolver for the host ID.
	Resolver hostIDResolver `yaml:"resolver"`

	// Value is the config specified host ID if using config host ID resolver.
	Value *string `yaml:"value"`

	// EnvVarName is the environemnt specified host ID if using environment host ID resolver.
	EnvVarName *string `yaml:"envVarName"`
}

func (c hostIDConfiguration) Resolve() (string, error) {
	switch c.Resolver {
	case hostnameHostIDResolver:
		return os.Hostname()
	case configHostIDResolver:
		if c.Value == nil {
			err := fmt.Errorf("missing host ID using: resolver=%s",
				string(c.Resolver))
			return "", err
		}
		return *c.Value, nil
	case environmentHostIDResolver:
		if c.EnvVarName == nil {
			err := fmt.Errorf("missing host ID env var name using: resolver=%s",
				string(c.Resolver))
			return "", err
		}
		v := os.Getenv(*c.EnvVarName)
		if v == "" {
			err := fmt.Errorf("missing host ID env var value using: resolver=%s, name=%s",
				string(c.Resolver), *c.EnvVarName)
			return "", err
		}
		return v, nil
	}
	return "", fmt.Errorf("unknown host ID resolver: resolver=%s",
		string(c.Resolver))
}

type clientConfigurationOption func(v client.Options) client.Options

type clientConfigurationAdminOption func(v client.AdminOptions) client.AdminOptions

type clientConfiguration struct {
	// ConfigService is used when a topology initializer is not supplied.
	ConfigService etcdclient.Configuration `yaml:"configService"`

	// WriteTimeout is the write request timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout" validate:"min=0"`

	// FetchTimeout is the fetch request timeout.
	FetchTimeout time.Duration `yaml:"fetchTimeout" validate:"min=0"`

	// ConnectTimeout is the cluster connect timeout.
	ConnectTimeout time.Duration `yaml:"connectTimeout" validate:"min=0"`

	// WriteRetry is the write retry config.
	WriteRetry retry.Configuration `yaml:"writeRetry"`

	// FetchRetry is the fetch retry config.
	FetchRetry retry.Configuration `yaml:"fetchRetry"`

	// BackgroundHealthCheckFailLimit is the amount of times a background check
	// must fail before a connection is taken out of consideration.
	BackgroundHealthCheckFailLimit int `yaml:"backgroundHealthCheckFailLimit" validate:"min=1,max=10"`

	// BackgroundHealthCheckFailThrottleFactor is the factor of the host connect
	// time to use when sleeping between a failed health check and the next check.
	BackgroundHealthCheckFailThrottleFactor float64 `yaml:"backgroundHealthCheckFailThrottleFactor"`
}

type clientConfigurationRequirements struct {
	// InstrumentOptions is a required argument when
	// constructing a client from configuration.
	InstrumentOptions instrument.Options

	// TopologyInitializer is an optional argument when
	// constructing a client from configuration.
	TopologyInitializer topology.Initializer

	// EncodingOptions is an optional argument when
	// constructing a client from configuration.
	EncodingOptions encoding.Options
}

func (c clientConfiguration) NewClient(
	requirements clientConfigurationRequirements,
	custom ...clientConfigurationOption,
) (client.Client, error) {
	customAdmin := make([]clientConfigurationAdminOption, 0, len(custom))
	for _, opt := range custom {
		customAdmin = append(customAdmin, func(v client.AdminOptions) client.AdminOptions {
			return opt(client.Options(v)).(client.AdminOptions)
		})
	}

	v, err := c.NewAdminClient(requirements, customAdmin...)
	if err != nil {
		return nil, err
	}

	return v, err
}

func (c clientConfiguration) NewAdminClient(
	requirements clientConfigurationRequirements,
	custom ...clientConfigurationAdminOption,
) (client.AdminClient, error) {
	iopts := requirements.InstrumentOptions
	writeRequestScope := iopts.MetricsScope().SubScope("write-req")
	fetchRequestScope := iopts.MetricsScope().SubScope("fetch-req")

	topoInit := requirements.TopologyInitializer
	if topoInit == nil {
		configSvcClient, err := c.ConfigService.NewClient(iopts)
		if err != nil {
			return nil, err
		}

		initOpts := topology.NewDynamicOptions().
			SetConfigServiceClient(configSvcClient).
			SetServiceID(services.NewServiceID().
				SetName(c.ConfigService.Service).
				SetEnvironment(c.ConfigService.Env).
				SetZone(c.ConfigService.Zone)).
			SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
			SetInstrumentOptions(iopts)

		topoInit = topology.NewDynamicInitializer(initOpts)
	}

	v := client.NewAdminOptions().
		SetTopologyInitializer(topoInit).
		SetWriteConsistencyLevel(topology.ConsistencyLevelMajority).          // TODO(r): create unmarshal yaml for this type and read from config
		SetReadConsistencyLevel(client.ReadConsistencyLevelUnstrictMajority). // TODO(r): create unmarshal yaml for this type and read from config
		SetBackgroundHealthCheckFailLimit(c.BackgroundHealthCheckFailLimit).
		SetBackgroundHealthCheckFailThrottleFactor(c.BackgroundHealthCheckFailThrottleFactor).
		SetWriteRequestTimeout(c.WriteTimeout).
		SetFetchRequestTimeout(c.FetchTimeout).
		SetClusterConnectTimeout(c.ConnectTimeout).
		SetWriteRetrier(c.WriteRetry.NewRetrier(writeRequestScope)).
		SetFetchRetrier(c.FetchRetry.NewRetrier(fetchRequestScope)).
		SetChannelOptions(&tchannel.ChannelOptions{
			Logger: NewTChannelNoopLogger(),
		})

	encodingOpts := requirements.EncodingOptions
	if encodingOpts == nil {
		encodingOpts = encoding.NewOptions()
	}

	v = v.SetReaderIteratorAllocate(func(r io.Reader) encoding.ReaderIterator {
		intOptimized := m3tsz.DefaultIntOptimizationEnabled
		return m3tsz.NewReaderIterator(r, intOptimized, encodingOpts)
	})

	// Apply programtic custom options
	opts := v.(client.AdminOptions)
	for _, opt := range custom {
		opts = opt(opts)
	}

	return client.NewAdminClient(opts)
}

// TODO(r): place this logger in a different package
var tchannelNoLog tchannelNoopLogger

type tchannelNoopLogger struct{}

// NewTChannelNoopLogger returns a default tchannel no-op logger
func NewTChannelNoopLogger() tchannel.Logger { return tchannelNoLog }

func (tchannelNoopLogger) Enabled(_ tchannel.LogLevel) bool                         { return false }
func (tchannelNoopLogger) Fatal(msg string)                                         { os.Exit(1) }
func (tchannelNoopLogger) Error(msg string)                                         {}
func (tchannelNoopLogger) Warn(msg string)                                          {}
func (tchannelNoopLogger) Infof(msg string, args ...interface{})                    {}
func (tchannelNoopLogger) Info(msg string)                                          {}
func (tchannelNoopLogger) Debugf(msg string, args ...interface{})                   {}
func (tchannelNoopLogger) Debug(msg string)                                         {}
func (tchannelNoopLogger) Fields() tchannel.LogFields                               { return nil }
func (l tchannelNoopLogger) WithFields(fields ...tchannel.LogField) tchannel.Logger { return l }

type configuration struct {
	// Logging configuration.
	Logging xlog.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// The host and port on which to listen
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the cluster service
	HTTPClusterListenAddress string `yaml:"httpClusterListenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the node service
	HTTPNodeListenAddress string `yaml:"httpNodeListenAddress" validate:"nonzero"`

	// The host and port on which to listen for debug endpoints
	DebugListenAddress string `yaml:"debugListenAddress"`

	// HostID is the local host ID configuration.
	HostID hostIDConfiguration `yaml:"hostID"`

	// Client configuration, used for inter-node communication and when used as a coordinator.
	Client clientConfiguration `yaml:"client"`

	// The initial garbage collection target percentage
	GCPercentage int `yaml:"gcPercentage" validate:"max=100"`

	// Write new series asynchronously for fast ingestion of new ID bursts
	WriteNewSeriesAsync bool `yaml:"writeNewSeriesAsync"`

	// Write new series limit per second to limit overwhelming during new ID bursts
	WriteNewSeriesLimitPerSecond int `yaml:"writeNewSeriesLimitPerSecond"`

	// Write new series backoff between batches of new series insertions
	WriteNewSeriesBackoffDuration time.Duration `yaml:"writeNewSeriesBackoffDuration"`

	// Bootstrap configuration
	Bootstrap bconfig.BootstrapConfiguration `yaml:"bootstrap"`

	// TickInterval controls the tick interval for the node
	TickInterval time.Duration `yaml:"tickInterval" validate:"nonzero"`

	// The commit log policy for the node
	CommitLog commitLogPolicy `yaml:"commitlog"`

	// The filesystem policy for the node
	Filesystem fsPolicy `yaml:"fs"`

	// The repair policy for repairing in-memory data
	Repair repairPolicy `yaml:"repair"`

	// The pooling policy
	PoolingPolicy poolingPolicy `yaml:"poolingPolicy"`

	// The configuration for config service client
	ConfigService etcdclient.Configuration `yaml:"configService"`
}

type commitLogPolicy struct {
	// The max size the commit log will flush a segment to disk after buffering
	FlushMaxBytes int `yaml:"flushMaxBytes" validate:"nonzero"`

	// The maximum amount of time the commit log will wait to flush to disk
	FlushEvery time.Duration `yaml:"flushEvery" validate:"nonzero"`

	// The queue the commit log will keep in front of the current commit log segment
	Queue commitLogQueuePolicy `yaml:"queue" validate:"nonzero"`

	// The commit log retention policy
	RetentionPeriod time.Duration `yaml:"retentionPeriod" validate:"nonzero"`

	// The commit log block size
	BlockSize time.Duration `yaml:"blockSize" validate:"nonzero"`
}

type commitLogQueuePolicy struct {
	// The type of calculation for the size
	CalculationType string `yaml:"calculationType"`

	// The size of the commit log, calculated according to the calculation type
	Size int `yaml:"size" validate:"nonzero"`
}

type fsPolicy struct {
	// File path prefix for reading/writing TSDB files
	FilePathPrefix string `yaml:"filePathPrefix" validate:"nonzero"`

	// Write buffer size
	WriteBufferSize int `yaml:"writeBufferSize" validate:"min=0"`

	// Read buffer size
	ReadBufferSize int `yaml:"readBufferSize" validate:"min=0"`

	// Disk flush throughput limit in Mb/s
	ThroughputLimitMbps float64 `yaml:"throughputLimitMbps" validate:"min=0.0"`

	// Disk flush throughput check interval
	ThroughputCheckEvery int `yaml:"throughputCheckEvery" validate:"nonzero"`
}

type repairPolicy struct {
	// Enabled or disabled
	Enabled bool `yaml:"enabled"`

	// The repair interval
	Interval time.Duration `yaml:"interval" validate:"nonzero"`

	// The repair time offset
	Offset time.Duration `yaml:"offset" validate:"nonzero"`

	// The repair time jitter
	Jitter time.Duration `yaml:"jitter" validate:"nonzero"`

	// The repair throttle
	Throttle time.Duration `yaml:"throttle" validate:"nonzero"`

	// The repair check interval
	CheckInterval time.Duration `yaml:"checkInterval" validate:"nonzero"`
}

type poolingPolicy struct {
	// The initial alloc size for a block
	BlockAllocSize int `yaml:"blockAllocSize"`

	// The general pool type: simple or native.
	Type string `yaml:"type" validate:"regexp=(^simple$|^native$)"`

	// The Bytes pool buckets to use
	BytesPool bucketPoolPolicy `yaml:"bytesPool"`

	// The policy for the Closers pool
	ClosersPool poolPolicy `yaml:"closersPool"`

	// The policy for the Context pool
	ContextPool poolPolicy `yaml:"contextPool"`

	// The policy for the DatabaseSeries pool
	SeriesPool poolPolicy `yaml:"seriesPool"`

	// The policy for the DatabaseBlock pool
	BlockPool poolPolicy `yaml:"blockPool"`

	// The policy for the Encoder pool
	EncoderPool poolPolicy `yaml:"encoderPool"`

	// The policy for the Iterator pool
	IteratorPool poolPolicy `yaml:"iteratorPool"`

	// The policy for the Segment Reader pool
	SegmentReaderPool poolPolicy `yaml:"segmentReaderPool"`

	// The policy for the Identifier pool
	IdentifierPool poolPolicy `yaml:"identifierPool"`

	// The policy for the fetchBlockMetadataResult pool
	FetchBlockMetadataResultsPool capacityPoolPolicy `yaml:"fetchBlockMetadataResultsPool"`

	// The policy for the fetchBlocksMetadataResultsPool pool
	FetchBlocksMetadataResultsPool capacityPoolPolicy `yaml:"fetchBlocksMetadataResultsPool"`

	// The policy for the hostBlockMetadataSlicePool pool
	HostBlockMetadataSlicePool capacityPoolPolicy `yaml:"hostBlockMetadataSlicePool"`

	// The policy for the blockMetadataPool pool
	BlockMetadataPool poolPolicy `yaml:"blockMetadataPool"`

	// The policy for the blockMetadataSlicePool pool
	BlockMetadataSlicePool capacityPoolPolicy `yaml:"blockMetadataSlicePool"`

	// The policy for the blocksMetadataPool pool
	BlocksMetadataPool poolPolicy `yaml:"blocksMetadataPool"`

	// The policy for the blocksMetadataSlicePool pool
	BlocksMetadataSlicePool capacityPoolPolicy `yaml:"blocksMetadataSlicePool"`
}

type poolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}

type capacityPoolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The capacity of items in the pool
	Capacity int `yaml:"capacity"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}

type bucketPoolPolicy struct {
	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`

	// The pool buckets sizes to use
	Buckets []poolingPolicyBytesPoolBucket `yaml:"buckets"`
}

type poolingPolicyBytesPoolBucket struct {
	// The capacity of each item in the bucket
	Capacity int `yaml:"capacity"`

	// The count of the items in the bucket
	Count int `yaml:"count"`
}

var (
	configFile = flag.String("f", "", "configuration file")
)

func main() {
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg configuration
	if err := config.LoadFile(&cfg, *configFile); err != nil {
		fmt.Fprintf(os.Stderr, "unable to load %s: %v", *configFile, err)
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
	}

	debug.SetGCPercent(cfg.GCPercentage)

	scope, _, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("could not connect to metrics: %v", err)
	}

	opts := storage.NewOptions()

	iopts := opts.InstrumentOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(cfg.Metrics.SampleRate())
	opts = opts.SetInstrumentOptions(iopts)

	buildReporter := instrument.NewBuildReporter(iopts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatalf("unable to start build reporter: %v", err)
	}
	defer buildReporter.Close()

	csClient, err := etcdclient.NewConfigServiceClient(
		cfg.ConfigService.NewOptions().
			SetInstrumentOptions(
				instrument.NewOptions().
					SetLogger(logger).
					SetMetricsScope(scope),
			),
	)
	if err != nil {
		logger.Fatalf("could not create m3cluster client: %v", err)
	}

	dynamicOpts := namespace.NewDynamicOptions().
		SetInstrumentOptions(iopts).
		SetConfigServiceClient(csClient).
		SetNamespaceRegistryKey(KVStoreNamespacesKey)
	nsInit := namespace.NewDynamicInitializer(dynamicOpts)

	opts = opts.
		SetTickInterval(cfg.TickInterval).
		SetNamespaceInitializer(nsInit)

	fsopts := fs.NewOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope.SubScope("database.fs"))).
		SetFilePathPrefix(cfg.Filesystem.FilePathPrefix).
		SetNewFileMode(os.FileMode(0666)).
		SetNewDirectoryMode(os.ModeDir | os.FileMode(0755)).
		SetWriterBufferSize(cfg.Filesystem.WriteBufferSize).
		SetReaderBufferSize(cfg.Filesystem.ReadBufferSize)

	commitLogQueueSize := cfg.CommitLog.Queue.Size
	if cfg.CommitLog.Queue.CalculationType == string(calculationTypePerCPU) {
		commitLogQueueSize = commitLogQueueSize * runtime.NumCPU()
	}

	opts = opts.SetCommitLogOptions(opts.CommitLogOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetFilesystemOptions(fsopts).
		SetStrategy(commitlog.StrategyWriteBehind).
		SetFlushSize(cfg.CommitLog.FlushMaxBytes).
		SetFlushInterval(cfg.CommitLog.FlushEvery).
		SetBacklogQueueSize(commitLogQueueSize).
		SetRetentionPeriod(cfg.CommitLog.RetentionPeriod).
		SetBlockSize(cfg.CommitLog.BlockSize))

	runtimeOptsMgr := m3dbruntime.NewOptionsManager(m3dbruntime.NewOptions().
		SetPersistRateLimitOptions(ratelimit.NewOptions().
			SetLimitEnabled(true).
			SetLimitMbps(cfg.Filesystem.ThroughputLimitMbps).
			SetLimitCheckEvery(cfg.Filesystem.ThroughputCheckEvery)).
		SetWriteNewSeriesAsync(cfg.WriteNewSeriesAsync).
		SetWriteNewSeriesBackoffDuration(cfg.WriteNewSeriesBackoffDuration))
	defer runtimeOptsMgr.Close()

	opts = opts.SetRuntimeOptionsManager(runtimeOptsMgr)

	opts = withEncodingAndPoolingOptions(logger, opts, cfg.PoolingPolicy)

	// Set the block retriever manager
	retrieverOpts := fs.NewBlockRetrieverOptions().
		SetBytesPool(opts.BytesPool()).
		SetSegmentReaderPool(opts.SegmentReaderPool()).
		SetFetchConcurrency(2)
	blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
		func(md namespace.Metadata) (block.DatabaseBlockRetriever, error) {
			retriever := fs.NewBlockRetriever(retrieverOpts, fsopts)
			if err := retriever.Open(md); err != nil {
				return nil, err
			}
			return retriever, nil
		})
	opts = opts.SetDatabaseBlockRetrieverManager(blockRetrieverMgr)

	// Set the persistence manager
	opts = opts.SetPersistManager(fs.NewPersistManager(fsopts))

	logger.Info("creating config service client with m3kv")

	serviceID := services.NewServiceID().
		SetName(cfg.ConfigService.Service).
		SetEnvironment(cfg.ConfigService.Env).
		SetZone(cfg.ConfigService.Zone)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(csClient).
		SetServiceID(serviceID).
		SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
		SetInstrumentOptions(opts.InstrumentOptions())

	topoInit := topology.NewDynamicInitializer(topoOpts)
	topo, err := topoInit.Init()
	if err != nil {
		logger.Fatalf("could not initialize m3db topology: %v", err)
	}

	hostID, err := cfg.HostID.Resolve()
	if err != nil {
		logger.Fatalf("could not resolve local host ID: %v", err)
	}

	m3dbClient, err := cfg.Client.NewAdminClient(
		clientConfigurationRequirements{
			InstrumentOptions: iopts.
				SetMetricsScope(iopts.MetricsScope().SubScope("m3dbclient")),
			TopologyInitializer: topoInit,
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetContextPool(opts.ContextPool()).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetOrigin(topology.NewHost(hostID, ""))
		})
	if err != nil {
		logger.Fatalf("could not create m3db client: %v", err)
	}

	kv, err := csClient.KV()
	if err != nil {
		logger.Fatalf("could not create KV client, %v", err)
	}

	// Set bootstrap options
	bs, err := cfg.Bootstrap.New(opts, m3dbClient, blockRetrieverMgr)
	if err != nil {
		logger.Fatalf("could not create bootstrap process: %v", err)
	}

	opts = opts.SetBootstrapProcess(bs)

	timeout := bootstrapConfigInitTimeout
	updateBootstrappersFromM3KV(kv, logger, timeout, cfg.Bootstrap.Bootstrappers,
		func(bootstrappers []string) {
			if len(bootstrappers) == 0 {
				logger.Errorf("updated bootstrapper list is empty")
				return
			}

			cfg.Bootstrap.Bootstrappers = bootstrappers
			updated, err := cfg.Bootstrap.New(opts, m3dbClient, blockRetrieverMgr)
			if err != nil {
				logger.Errorf("updated bootstrapper list failed: %v", err)
				return
			}

			bs.SetBootstrapper(updated.Bootstrapper())
		})

	// Set repair options
	policy := cfg.PoolingPolicy
	hostBlockMetadataSlicePool := repair.NewHostBlockMetadataSlicePool(
		capacityPoolOptions(policy.HostBlockMetadataSlicePool, scope.SubScope("host-block-metadata-slice-pool")),
		policy.HostBlockMetadataSlicePool.Capacity,
	)
	opts = opts.
		SetRepairEnabled(cfg.Repair.Enabled).
		SetRepairOptions(opts.RepairOptions().
			SetAdminClient(m3dbClient).
			SetRepairInterval(cfg.Repair.Interval).
			SetRepairTimeOffset(cfg.Repair.Offset).
			SetRepairTimeJitter(cfg.Repair.Jitter).
			SetRepairThrottle(cfg.Repair.Throttle).
			SetRepairCheckInterval(cfg.Repair.CheckInterval).
			SetHostBlockMetadataSlicePool(hostBlockMetadataSlicePool))

	// Set tchannelthrift options
	blockMetadataPool := tchannelthrift.NewBlockMetadataPool(
		poolOptions(policy.BlockMetadataPool, scope.SubScope("block-metadata-pool")),
	)
	blockMetadataSlicePool := tchannelthrift.NewBlockMetadataSlicePool(
		capacityPoolOptions(policy.BlockMetadataSlicePool, scope.SubScope("block-metadata-slice-pool")),
		policy.BlockMetadataSlicePool.Capacity,
	)
	blocksMetadataPool := tchannelthrift.NewBlocksMetadataPool(
		poolOptions(policy.BlocksMetadataPool, scope.SubScope("blocks-metadata-pool")),
	)
	blocksMetadataSlicePool := tchannelthrift.NewBlocksMetadataSlicePool(
		capacityPoolOptions(policy.BlocksMetadataSlicePool, scope.SubScope("blocks-metadata-slice-pool")),
		policy.BlocksMetadataSlicePool.Capacity,
	)

	ttopts := tchannelthrift.NewOptions().
		SetBlockMetadataPool(blockMetadataPool).
		SetBlockMetadataSlicePool(blockMetadataSlicePool).
		SetBlocksMetadataPool(blocksMetadataPool).
		SetBlocksMetadataSlicePool(blocksMetadataSlicePool)

	db, err := cluster.NewDatabase(hostID, topoInit, opts)
	if err != nil {
		logger.Fatalf("could not construct database: %v", err)
	}
	if err := db.Open(); err != nil {
		logger.Fatalf("could not open database: %v", err)
	}

	contextPool := opts.ContextPool()

	tchannelOpts := &tchannel.ChannelOptions{
		Logger: NewTChannelNoopLogger(),
	}
	tchannelthriftNodeClose, err := ttnode.NewServer(db, cfg.ListenAddress, contextPool, tchannelOpts, ttopts).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open tchannelthrift interface: %v", err)
	}
	defer tchannelthriftNodeClose()
	logger.Infof("node tchannelthrift: listening on %v", cfg.ListenAddress)

	httpjsonNodeClose, err := hjnode.NewServer(db, cfg.HTTPNodeListenAddress, contextPool, nil, ttopts).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open httpjson interface: %v", err)
	}
	defer httpjsonNodeClose()
	logger.Infof("node httpjson: listening on %v", cfg.HTTPNodeListenAddress)

	httpjsonClusterClose, err := hjcluster.NewServer(m3dbClient, cfg.HTTPClusterListenAddress, contextPool, nil).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open httpjson interface: %v", err)
	}
	defer httpjsonClusterClose()
	logger.Infof("cluster httpjson: listening on %v", cfg.HTTPClusterListenAddress)

	if cfg.DebugListenAddress != "" {
		go func() {
			if err := http.ListenAndServe(cfg.DebugListenAddress, nil); err != nil {
				logger.Errorf("debug server could not listen on %s: %v", cfg.DebugListenAddress, err)
			}
		}()
	}

	go func() {
		// Bootstrap asynchronously so we can handle interrupt
		if err := db.Bootstrap(); err != nil {
			logger.Fatalf("could not bootstrap database: %v", err)
		}
		logger.Infof("bootstrapped")

		// Only set the write new series limit after bootstrapping
		updateNewSeriesLimitPerShardFromM3KV(kv, logger, topo,
			runtimeOptsMgr, cfg.WriteNewSeriesLimitPerSecond)
	}()

	// Handle interrupt
	logger.Warnf("interrupt: %v", interrupt())

	// Attempt graceful server close
	cleanCloseTimeout := 10 * time.Second

	closedCh := make(chan struct{})
	go func() {
		err := db.Terminate()
		if err != nil {
			logger.Errorf("close database error: %v", err)
		}
		closedCh <- struct{}{}
	}()

	// Wait then close or hard close
	select {
	case <-closedCh:
		logger.Infof("server closed")
	case <-time.After(cleanCloseTimeout):
		logger.Errorf("server closed after %s timeout", cleanCloseTimeout.String())
	}
}

func updateNewSeriesLimitPerShardFromM3KV(
	store kv.Store,
	logger xlog.Logger,
	topo topology.Topology,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	defaultClusterNewSeriesLimit int,
) {
	var initClusterLimit int

	value, err := store.Get(KVStoreClusterNewSeriesInsertLimit)
	if err == nil {
		protoValue := &commonpb.Int64Proto{}
		err = value.Unmarshal(protoValue)
		if err == nil {
			initClusterLimit = int(protoValue.Value)
		}
	}

	if err != nil {
		if err != kv.ErrNotFound {
			logger.Warnf("error resolving cluster new series insert limit: %v", err)
		}
		initClusterLimit = defaultClusterNewSeriesLimit
	}

	setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, initClusterLimit)

	watch, err := store.Watch(KVStoreClusterNewSeriesInsertLimit)
	if err != nil {
		logger.Errorf("could not watch cluster new series insert limit: %v", err)
		return
	}

	go func() {
		protoValue := &commonpb.Int64Proto{}
		for range watch.C() {
			err := watch.Get().Unmarshal(protoValue)
			if err != nil {
				logger.Warnf("unable to set cluster new series insert limit: %v", err)
				continue
			}

			value := int(protoValue.Value)
			setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, value)
		}
	}()
}

func setNewSeriesLimitPerShardOnChange(
	topo topology.Topology,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	clusterLimit int,
) {
	perPlacedShardLimit := clusterLimitToPlacedShardLimit(topo, clusterLimit)
	runtimeOpts := runtimeOptsMgr.Get()
	if runtimeOpts.WriteNewSeriesLimitPerShardPerSecond() == perPlacedShardLimit {
		// Not changed, no need to set the value and trigger a runtime options update
		return
	}

	runtimeOptsMgr.Update(runtimeOpts.
		SetWriteNewSeriesLimitPerShardPerSecond(perPlacedShardLimit))
}

func clusterLimitToPlacedShardLimit(topo topology.Topology, clusterLimit int) int {
	if clusterLimit < 1 {
		return 0
	}
	topoMap := topo.Get()
	numShards := len(topoMap.ShardSet().AllIDs())
	numPlacedShards := numShards * topoMap.Replicas()
	if numPlacedShards < 1 {
		return 0
	}
	nodeLimit := int(math.Ceil(
		float64(clusterLimit) / float64(numPlacedShards)))
	return nodeLimit
}

// this function will block for at most waitTimeout to try to get an initial value
// before we kick off the bootstrap
func updateBootstrappersFromM3KV(
	kv kv.Store,
	logger xlog.Logger,
	waitTimeout time.Duration,
	defaultBootstrappers []string,
	onUpdate func(bootstrappers []string),
) {
	vw, err := kv.Watch(KVStoreBootstrapperKey)
	if err != nil {
		logger.Fatalf("could not watch value for key with KV: %s", KVStoreBootstrapperKey)
	}

	initializedCh := make(chan struct{})

	var initialized bool
	go func() {
		opts := util.NewOptions().SetLogger(logger)

		for range vw.C() {
			v, err := util.StringArrayFromValue(
				vw.Get(), KVStoreBootstrapperKey, defaultBootstrappers, opts,
			)
			if err != nil {
				logger.WithFields(
					xlog.NewField("key", KVStoreBootstrapperKey),
					xlog.NewErrField(err),
				).Error("error converting KV update to string array")
				continue
			}

			onUpdate(v)

			if !initialized {
				initialized = true
				close(initializedCh)
			}
		}
	}()

	select {
	case <-time.After(waitTimeout):
	case <-initializedCh:
	}
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}

func withEncodingAndPoolingOptions(
	logger xlog.Logger,
	opts storage.Options,
	policy poolingPolicy,
) storage.Options {
	scope := opts.InstrumentOptions().MetricsScope()

	logger.Infof("using %s pools", policy.Type)

	buckets := make([]pool.Bucket, len(policy.BytesPool.Buckets))
	for i, bucket := range policy.BytesPool.Buckets {
		buckets[i] = pool.Bucket{
			Capacity: bucket.Capacity,
			Count:    bucket.Count,
		}
		logger.Infof("bytes pool registering bucket capacity=%d, count=%d",
			bucket.Capacity, bucket.Count)
	}

	var bytesPool pool.CheckedBytesPool

	switch policy.Type {
	case "simple":
		bytesPoolOpts := pool.NewObjectPoolOptions().
			SetRefillLowWatermark(policy.BytesPool.RefillLowWaterMark).
			SetRefillHighWatermark(policy.BytesPool.RefillHighWaterMark).
			SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope.SubScope("checked-bytes-pool")))
		bytesPool = pool.NewCheckedBytesPool(buckets, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, bytesPoolOpts.
				SetInstrumentOptions(bytesPoolOpts.InstrumentOptions().SetMetricsScope(scope.SubScope("bytes-pool"))))
		})
	case "native":
		bytesPoolOpts := pool.NewObjectPoolOptions().
			SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope.SubScope("checked-bytes-pool")))
		bytesPool = pool.NewCheckedBytesPool(buckets, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewNativeHeap(s, bytesPoolOpts.
				SetInstrumentOptions(bytesPoolOpts.InstrumentOptions().SetMetricsScope(scope.SubScope("bytes-pool"))))
		})
	}

	bytesPool.Init()

	segmentReaderPool := m3dbxio.NewSegmentReaderPool(
		poolOptions(policy.SegmentReaderPool, scope.SubScope("segment-reader-pool")))
	segmentReaderPool.Init()
	encoderPool := encoding.NewEncoderPool(
		poolOptions(policy.EncoderPool, scope.SubScope("encoder-pool")))
	closersPoolOpts := poolOptions(policy.ClosersPool, scope.SubScope("closers-pool"))
	contextPoolOpts := poolOptions(policy.ContextPool, scope.SubScope("context-pool"))
	contextPool := context.NewPool(contextPoolOpts, closersPoolOpts)
	iteratorPool := encoding.NewReaderIteratorPool(
		poolOptions(policy.IteratorPool, scope.SubScope("iterator-pool")))
	multiIteratorPool := encoding.NewMultiReaderIteratorPool(
		poolOptions(policy.IteratorPool, scope.SubScope("multi-iterator-pool")))

	var identifierPool ts.IdentifierPool

	switch policy.Type {
	case "simple":
		identifierPool = ts.NewIdentifierPool(
			bytesPool,
			poolOptions(policy.IdentifierPool, scope.SubScope("identifier-pool")))
	case "native":
		identifierPool = ts.NewNativeIdentifierPool(
			bytesPool,
			poolOptions(policy.IdentifierPool, scope.SubScope("identifier-pool")))
	}

	fetchBlockMetadataResultsPool := block.NewFetchBlockMetadataResultsPool(
		capacityPoolOptions(policy.FetchBlockMetadataResultsPool, scope.SubScope("fetch-block-metadata-results-pool")),
		policy.FetchBlockMetadataResultsPool.Capacity,
	)
	fetchBlocksMetadataResultsPool := block.NewFetchBlocksMetadataResultsPool(
		capacityPoolOptions(policy.FetchBlocksMetadataResultsPool, scope.SubScope("fetch-blocks-metadata-results-pool")),
		policy.FetchBlocksMetadataResultsPool.Capacity,
	)

	encodingOpts := encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetBytesPool(bytesPool).
		SetSegmentReaderPool(segmentReaderPool)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(timeZero, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	iteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	multiIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		iter := iteratorPool.Get()
		iter.Reset(r)
		return iter
	})

	opts = opts.
		SetBytesPool(bytesPool).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
		SetIdentifierPool(identifierPool).
		SetFetchBlockMetadataResultsPool(fetchBlockMetadataResultsPool).
		SetFetchBlocksMetadataResultsPool(fetchBlocksMetadataResultsPool)

	blockOpts := opts.DatabaseBlockOptions().
		SetDatabaseBlockAllocSize(policy.BlockAllocSize).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetSegmentReaderPool(segmentReaderPool).
		SetBytesPool(bytesPool)
	blockPool := block.NewDatabaseBlockPool(poolOptions(policy.BlockPool, scope.SubScope("block-pool")))
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(timeZero, ts.Segment{}, blockOpts)
	})
	blockOpts = blockOpts.SetDatabaseBlockPool(blockPool)
	opts = opts.SetDatabaseBlockOptions(blockOpts)

	// NB(prateek): retention opts are overriden per namespace during series creation
	retentionOpts := retention.NewOptions()
	seriesOpts := storage.NewSeriesOptionsFromOptions(opts, retentionOpts).
		SetFetchBlockMetadataResultsPool(opts.FetchBlockMetadataResultsPool())
	seriesPool := series.NewDatabaseSeriesPool(
		poolOptions(policy.SeriesPool, scope.SubScope("series-pool")))
	opts = opts.
		SetSeriesOptions(seriesOpts).
		SetDatabaseSeriesPool(seriesPool)
	opts = opts.SetCommitLogOptions(opts.CommitLogOptions().
		SetBytesPool(bytesPool))

	return opts
}

func poolOptions(policy poolPolicy, scope tally.Scope) pool.ObjectPoolOptions {
	opts := pool.NewObjectPoolOptions()
	if policy.Size > 0 {
		opts = opts.SetSize(policy.Size)
		if policy.RefillLowWaterMark > 0 &&
			policy.RefillHighWaterMark > 0 &&
			policy.RefillHighWaterMark > policy.RefillLowWaterMark {
			opts = opts.SetRefillLowWatermark(policy.RefillLowWaterMark)
			opts = opts.SetRefillHighWatermark(policy.RefillHighWaterMark)
		}
	}
	if scope != nil {
		opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))
	}
	return opts
}

func capacityPoolOptions(policy capacityPoolPolicy, scope tally.Scope) pool.ObjectPoolOptions {
	opts := pool.NewObjectPoolOptions()
	if policy.Size > 0 {
		opts = opts.SetSize(policy.Size)
		if policy.RefillLowWaterMark > 0 &&
			policy.RefillHighWaterMark > 0 &&
			policy.RefillHighWaterMark > policy.RefillLowWaterMark {
			opts = opts.SetRefillLowWatermark(policy.RefillLowWaterMark)
			opts = opts.SetRefillHighWatermark(policy.RefillHighWaterMark)
		}
	}
	if scope != nil {
		opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))
	}
	return opts
}
