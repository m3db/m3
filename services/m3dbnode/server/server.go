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

package server

import (
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
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
	"github.com/m3db/m3db/kvconfig"
	hjcluster "github.com/m3db/m3db/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3db/network/server/httpjson/node"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	ttcluster "github.com/m3db/m3db/network/server/tchannelthrift/cluster"
	ttnode "github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	m3dbruntime "github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/services/m3dbnode/config"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/cluster"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	m3dbxio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3db/x/tchannel"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"

	"github.com/uber-go/tally"
)

const (
	bootstrapConfigInitTimeout = 10 * time.Second
	serverGracefulCloseTimeout = 10 * time.Second
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the YAML configuration file to use to run the server.
	ConfigFile string

	// BootstrapCh is a channel to listen on to be notified of bootstrap.
	BootstrapCh chan<- struct{}

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error
}

// Run runs the server programmatically given a filename for the
// configuration file.
func Run(runOpts RunOptions) {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, runOpts.ConfigFile); err != nil {
		fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
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
	defer buildReporter.Stop()

	newFileMode, err := cfg.Filesystem.ParseNewFileMode()
	if err != nil {
		log.Fatalf("could not parse new file mode: %v", err)
	}

	newDirectoryMode, err := cfg.Filesystem.ParseNewDirectoryMode()
	if err != nil {
		log.Fatalf("could not parse new directory mode: %v", err)
	}

	fsopts := fs.NewOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions().
			SetMetricsScope(scope.SubScope("database.fs"))).
		SetFilePathPrefix(cfg.Filesystem.FilePathPrefix).
		SetNewFileMode(newFileMode).
		SetNewDirectoryMode(newDirectoryMode).
		SetWriterBufferSize(cfg.Filesystem.WriteBufferSize).
		SetDataReaderBufferSize(cfg.Filesystem.DataReadBufferSize).
		SetInfoReaderBufferSize(cfg.Filesystem.InfoReadBufferSize).
		SetSeekReaderBufferSize(cfg.Filesystem.SeekReadBufferSize)

	var commitLogQueueSize int
	specified := cfg.CommitLog.Queue.Size
	switch cfg.CommitLog.Queue.CalculationType {
	case config.CalculationTypeFixed:
		commitLogQueueSize = specified
	case config.CalculationTypePerCPU:
		commitLogQueueSize = specified * runtime.NumCPU()
	default:
		log.Fatalf("unknown commit log queue size type: %v",
			cfg.CommitLog.Queue.CalculationType)
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
		SetSegmentReaderPool(opts.SegmentReaderPool())
	if cfg.BlockRetrieve.FetchConcurrency > 0 {
		retrieverOpts = retrieverOpts.
			SetFetchConcurrency(cfg.BlockRetrieve.FetchConcurrency)
	}
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

	logger.Info("creating config service client with m3cluster ")
	configSvcClientOpts := cfg.ConfigService.NewOptions().
		SetInstrumentOptions(
			instrument.NewOptions().
				SetLogger(logger).
				SetMetricsScope(scope))
	configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
	if err != nil {
		logger.Fatalf("could not create m3cluster client: %v", err)
	}

	dynamicOpts := namespace.NewDynamicOptions().
		SetInstrumentOptions(iopts).
		SetConfigServiceClient(configSvcClient).
		SetNamespaceRegistryKey(kvconfig.NamespacesKey)
	nsInit := namespace.NewDynamicInitializer(dynamicOpts)

	opts = opts.
		SetTickInterval(cfg.TickInterval).
		SetNamespaceInitializer(nsInit)

	serviceID := services.NewServiceID().
		SetName(cfg.ConfigService.Service).
		SetEnvironment(cfg.ConfigService.Env).
		SetZone(cfg.ConfigService.Zone)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(configSvcClient).
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
		client.ConfigurationParameters{
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

	kv, err := configSvcClient.KV()
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
	kvWatchBootstrappers(kv, logger, timeout, cfg.Bootstrap.Bootstrappers,
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
		capacityPoolOptions(policy.HostBlockMetadataSlicePool,
			scope.SubScope("host-block-metadata-slice-pool")),
		policy.HostBlockMetadataSlicePool.Capacity)

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
		poolOptions(policy.BlockMetadataPool, scope.SubScope("block-metadata-pool")))
	blockMetadataSlicePool := tchannelthrift.NewBlockMetadataSlicePool(
		capacityPoolOptions(policy.BlockMetadataSlicePool, scope.SubScope("block-metadata-slice-pool")),
		policy.BlockMetadataSlicePool.Capacity)
	blocksMetadataPool := tchannelthrift.NewBlocksMetadataPool(
		poolOptions(policy.BlocksMetadataPool, scope.SubScope("blocks-metadata-pool")))
	blocksMetadataSlicePool := tchannelthrift.NewBlocksMetadataSlicePool(
		capacityPoolOptions(policy.BlocksMetadataSlicePool, scope.SubScope("blocks-metadata-slice-pool")),
		policy.BlocksMetadataSlicePool.Capacity)

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

	tchannelOpts := xtchannel.NewDefaultChannelOptions()
	tchannelthriftNodeClose, err := ttnode.NewServer(db,
		cfg.ListenAddress, contextPool, tchannelOpts, ttopts).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open tchannelthrift interface on %s: %v",
			cfg.ListenAddress, err)
	}
	defer tchannelthriftNodeClose()
	logger.Infof("node tchannelthrift: listening on %v", cfg.ListenAddress)

	tchannelthriftClusterClose, err := ttcluster.NewServer(m3dbClient,
		cfg.ClusterListenAddress, contextPool, tchannelOpts).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open tchannelthrift interface on %s: %v",
			cfg.ClusterListenAddress, err)
	}
	defer tchannelthriftClusterClose()
	logger.Infof("cluster tchannelthrift: listening on %v", cfg.ClusterListenAddress)

	httpjsonNodeClose, err := hjnode.NewServer(db,
		cfg.HTTPNodeListenAddress, contextPool, nil, ttopts).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open httpjson interface on %s: %v",
			cfg.HTTPNodeListenAddress, err)
	}
	defer httpjsonNodeClose()
	logger.Infof("node httpjson: listening on %v", cfg.HTTPNodeListenAddress)

	httpjsonClusterClose, err := hjcluster.NewServer(m3dbClient,
		cfg.HTTPClusterListenAddress, contextPool, nil).ListenAndServe()
	if err != nil {
		logger.Fatalf("could not open httpjson interface on %s: %v",
			cfg.HTTPClusterListenAddress, err)
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
		if runOpts.BootstrapCh != nil {
			// Notify on bootstrap chan if specified
			defer func() {
				runOpts.BootstrapCh <- struct{}{}
			}()
		}

		// Bootstrap asynchronously so we can handle interrupt
		if err := db.Bootstrap(); err != nil {
			logger.Fatalf("could not bootstrap database: %v", err)
		}
		logger.Infof("bootstrapped")

		// Only set the write new series limit after bootstrapping
		kvWatchNewSeriesLimitPerShard(kv, logger, topo,
			runtimeOptsMgr, cfg.WriteNewSeriesLimitPerSecond)
	}()

	// Handle interrupt
	interruptCh := runOpts.InterruptCh
	if interruptCh == nil {
		// Make a noop chan so we can always select
		interruptCh = make(chan error)
	}

	var interruptErr error
	select {
	case err := <-interruptCh:
		interruptErr = err
	case sig := <-interrupt():
		interruptErr = fmt.Errorf("%v", sig)
	}

	logger.Warnf("interrupt: %v", interruptErr)

	// Attempt graceful server close
	closedCh := make(chan struct{})
	go func() {
		err := db.Terminate()
		if err != nil {
			logger.Errorf("close database error: %v", err)
		}
		closedCh <- struct{}{}
	}()

	// Wait then close or hard close
	closeTimeout := serverGracefulCloseTimeout
	select {
	case <-closedCh:
		logger.Infof("server closed")
	case <-time.After(closeTimeout):
		logger.Errorf("server closed after %s timeout", closeTimeout.String())
	}
}

func interrupt() <-chan os.Signal {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return c
}

func kvWatchNewSeriesLimitPerShard(
	store kv.Store,
	logger xlog.Logger,
	topo topology.Topology,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	defaultClusterNewSeriesLimit int,
) {
	var initClusterLimit int

	value, err := store.Get(kvconfig.ClusterNewSeriesInsertLimitKey)
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

	watch, err := store.Watch(kvconfig.ClusterNewSeriesInsertLimitKey)
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
func kvWatchBootstrappers(
	kv kv.Store,
	logger xlog.Logger,
	waitTimeout time.Duration,
	defaultBootstrappers []string,
	onUpdate func(bootstrappers []string),
) {
	vw, err := kv.Watch(kvconfig.BootstrapperKey)
	if err != nil {
		logger.Fatalf("could not watch value for key with KV: %s",
			kvconfig.BootstrapperKey)
	}

	initializedCh := make(chan struct{})

	var initialized bool
	go func() {
		opts := util.NewOptions().SetLogger(logger)

		for range vw.C() {
			v, err := util.StringArrayFromValue(vw.Get(),
				kvconfig.BootstrapperKey, defaultBootstrappers, opts)
			if err != nil {
				logger.WithFields(
					xlog.NewField("key", kvconfig.BootstrapperKey),
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

func withEncodingAndPoolingOptions(
	logger xlog.Logger,
	opts storage.Options,
	policy config.PoolingPolicy,
) storage.Options {
	scope := opts.InstrumentOptions().MetricsScope()

	logger.Infof("using %s pools", policy.Type)

	buckets := make([]pool.Bucket, len(policy.BytesPool.Buckets))
	for i, bucket := range policy.BytesPool.Buckets {
		var b pool.Bucket
		b.Capacity = bucket.Capacity
		b.Count = bucket.Count
		buckets[i] = b
		logger.Infof("bytes pool registering bucket capacity=%d, count=%d",
			bucket.Capacity, bucket.Count)
	}

	var bytesPool pool.CheckedBytesPool

	switch policy.Type {
	case config.SimplePooling:
		bytesPoolOpts := pool.NewObjectPoolOptions().
			SetRefillLowWatermark(policy.BytesPool.RefillLowWaterMark).
			SetRefillHighWatermark(policy.BytesPool.RefillHighWaterMark).
			SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope.SubScope("checked-bytes-pool")))
		bytesPool = pool.NewCheckedBytesPool(buckets, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, bytesPoolOpts.
				SetInstrumentOptions(bytesPoolOpts.InstrumentOptions().SetMetricsScope(scope.SubScope("bytes-pool"))))
		})
	case config.NativePooling:
		bytesPoolOpts := pool.NewObjectPoolOptions().
			SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope.SubScope("checked-bytes-pool")))
		bytesPool = pool.NewCheckedBytesPool(buckets, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewNativeHeap(s, bytesPoolOpts.
				SetInstrumentOptions(bytesPoolOpts.InstrumentOptions().SetMetricsScope(scope.SubScope("bytes-pool"))))
		})
	default:
		logger.Fatalf("unrecognized pooling type: %s", policy.Type)
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
		capacityPoolOptions(policy.FetchBlockMetadataResultsPool,
			scope.SubScope("fetch-block-metadata-results-pool")),
		policy.FetchBlockMetadataResultsPool.Capacity)

	fetchBlocksMetadataResultsPool := block.NewFetchBlocksMetadataResultsPool(
		capacityPoolOptions(policy.FetchBlocksMetadataResultsPool,
			scope.SubScope("fetch-blocks-metadata-results-pool")),
		policy.FetchBlocksMetadataResultsPool.Capacity)

	encodingOpts := encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetBytesPool(bytesPool).
		SetSegmentReaderPool(segmentReaderPool)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Time{}, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
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
	blockPool := block.NewDatabaseBlockPool(poolOptions(policy.BlockPool,
		scope.SubScope("block-pool")))
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, ts.Segment{}, blockOpts)
	})
	blockOpts = blockOpts.SetDatabaseBlockPool(blockPool)
	opts = opts.SetDatabaseBlockOptions(blockOpts)

	// NB(prateek): retention opts are overridden per namespace during series creation
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

func poolOptions(
	policy config.PoolPolicy,
	scope tally.Scope,
) pool.ObjectPoolOptions {
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
		opts = opts.SetInstrumentOptions(opts.InstrumentOptions().
			SetMetricsScope(scope))
	}
	return opts
}

func capacityPoolOptions(
	policy config.CapacityPoolPolicy,
	scope tally.Scope,
) pool.ObjectPoolOptions {
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
		opts = opts.SetInstrumentOptions(opts.InstrumentOptions().
			SetMetricsScope(scope))
	}
	return opts
}
