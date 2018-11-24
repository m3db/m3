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
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	hjcluster "github.com/m3db/m3/src/dbnode/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3/src/dbnode/network/server/httpjson/node"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	ttcluster "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/cluster"
	ttnode "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/ratelimit"
	"github.com/m3db/m3/src/dbnode/retention"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/cluster"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/tchannel"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xdocs "github.com/m3db/m3/src/x/docs"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/serialize"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
	"github.com/uber-go/tally"
)

const (
	bootstrapConfigInitTimeout       = 10 * time.Second
	serverGracefulCloseTimeout       = 10 * time.Second
	bgProcessLimitInterval           = 10 * time.Second
	maxBgProcessLimitMonitorDuration = 5 * time.Minute
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the YAML configuration file to use to run the server.
	ConfigFile string

	// Config is an alternate way to provide configuration and will be used
	// instead of parsing ConfigFile if ConfigFile is not specified.
	Config config.DBConfiguration

	// BootstrapCh is a channel to listen on to be notified of bootstrap.
	BootstrapCh chan<- struct{}

	// EmbeddedKVCh is a channel to listen on to be notified that the embedded KV has bootstrapped.
	EmbeddedKVCh chan<- struct{}

	// ClientCh is a channel to listen on to share the same m3db client that this server uses.
	ClientCh chan<- client.Client

	// ClusterClientCh is a channel to listen on to share the same m3 cluster client that this server uses.
	ClusterClientCh chan<- clusterclient.Client

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error
}

// Run runs the server programmatically given a filename for the
// configuration file.
func Run(runOpts RunOptions) {
	var cfg config.DBConfiguration
	if runOpts.ConfigFile != "" {
		var rootCfg config.Configuration
		if err := xconfig.LoadFile(&rootCfg, runOpts.ConfigFile, xconfig.Options{}); err != nil {
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
			os.Exit(1)
		}

		cfg = *rootCfg.DB
	} else {
		cfg = runOpts.Config
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}

	go bgValidateProcessLimits(logger)
	debug.SetGCPercent(cfg.GCPercentage)

	scope, _, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("could not connect to metrics: %v", err)
	}

	hostID, err := cfg.HostID.Resolve()
	if err != nil {
		logger.Fatalf("could not resolve local host ID: %v", err)
	}

	capnslog.SetGlobalLogLevel(capnslog.WARNING)

	// Presence of KV server config indicates embedded etcd cluster
	if cfg.EnvironmentConfig.SeedNodes == nil {
		logger.Info("no seed nodes set, using dedicated etcd cluster")
	} else {
		// Default etcd client clusters if not set already
		clusters := cfg.EnvironmentConfig.Service.ETCDClusters
		seedNodes := cfg.EnvironmentConfig.SeedNodes.InitialCluster
		if len(clusters) == 0 {
			endpoints, err := config.InitialClusterEndpoints(seedNodes)
			if err != nil {
				logger.Fatalf("unable to create etcd clusters: %v", err)
			}

			zone := cfg.EnvironmentConfig.Service.Zone

			logger.Infof("using seed nodes etcd cluster: zone=%s, endpoints=%v", zone, endpoints)
			cfg.EnvironmentConfig.Service.ETCDClusters = []etcd.ClusterConfig{etcd.ClusterConfig{
				Zone:      zone,
				Endpoints: endpoints,
			}}
		}

		seedNodeHostIDs := make([]string, 0, len(seedNodes))
		for _, entry := range seedNodes {
			seedNodeHostIDs = append(seedNodeHostIDs, entry.HostID)
		}
		logger.WithFields(
			xlog.NewField("hostID", hostID),
			xlog.NewField("seedNodeHostIDs", fmt.Sprintf("%v", seedNodeHostIDs)),
		).Info("resolving seed node configuration")

		if !config.IsSeedNode(seedNodes, hostID) {
			logger.Info("not a seed node, using cluster seed nodes")
		} else {
			logger.Info("seed node, starting etcd server")

			etcdCfg, err := config.NewEtcdEmbedConfig(cfg)
			if err != nil {
				logger.Fatalf("unable to create etcd config: %v", err)
			}

			e, err := embed.StartEtcd(etcdCfg)
			if err != nil {
				logger.Fatalf("could not start embedded etcd: %v", err)
			}

			if runOpts.EmbeddedKVCh != nil {
				// Notify on embedded KV bootstrap chan if specified
				runOpts.EmbeddedKVCh <- struct{}{}
			}

			defer e.Close()
		}
	}

	opts := storage.NewOptions()
	iopts := opts.InstrumentOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(cfg.Metrics.SampleRate())
	opts = opts.SetInstrumentOptions(iopts)

	if cfg.Index.MaxQueryIDsConcurrency != 0 {
		queryIDsWorkerPool := xsync.NewWorkerPool(cfg.Index.MaxQueryIDsConcurrency)
		queryIDsWorkerPool.Init()
		opts = opts.SetQueryIDsWorkerPool(queryIDsWorkerPool)
	} else {
		logger.Warnf("max index query IDs concurrency was not set, falling back to default value")
	}

	buildReporter := instrument.NewBuildReporter(iopts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatalf("unable to start build reporter: %v", err)
	}
	defer buildReporter.Stop()

	runtimeOpts := m3dbruntime.NewOptions().
		SetPersistRateLimitOptions(ratelimit.NewOptions().
			SetLimitEnabled(true).
			SetLimitMbps(cfg.Filesystem.ThroughputLimitMbps).
			SetLimitCheckEvery(cfg.Filesystem.ThroughputCheckEvery)).
		SetWriteNewSeriesAsync(cfg.WriteNewSeriesAsync).
		SetWriteNewSeriesBackoffDuration(cfg.WriteNewSeriesBackoffDuration)
	if lruCfg := cfg.Cache.SeriesConfiguration().LRU; lruCfg != nil {
		runtimeOpts = runtimeOpts.SetMaxWiredBlocks(lruCfg.MaxBlocks)
	}

	// FOLLOWUP(prateek): remove this once we have the runtime options<->index wiring done
	indexOpts := opts.IndexOptions()
	insertMode := index.InsertSync
	if cfg.WriteNewSeriesAsync {
		insertMode = index.InsertAsync
	}
	opts = opts.SetIndexOptions(
		indexOpts.SetInsertMode(insertMode))

	if tick := cfg.Tick; tick != nil {
		runtimeOpts = runtimeOpts.
			SetTickSeriesBatchSize(tick.SeriesBatchSize).
			SetTickPerSeriesSleepDuration(tick.PerSeriesSleepDuration).
			SetTickMinimumInterval(tick.MinimumInterval)
	}

	runtimeOptsMgr := m3dbruntime.NewOptionsManager()
	if err := runtimeOptsMgr.Update(runtimeOpts); err != nil {
		logger.Fatalf("could not set initial runtime options: %v", err)
	}
	defer runtimeOptsMgr.Close()

	opts = opts.SetRuntimeOptionsManager(runtimeOptsMgr)

	newFileMode, err := cfg.Filesystem.ParseNewFileMode()
	if err != nil {
		logger.Fatalf("could not parse new file mode: %v", err)
	}

	newDirectoryMode, err := cfg.Filesystem.ParseNewDirectoryMode()
	if err != nil {
		logger.Fatalf("could not parse new directory mode: %v", err)
	}

	mmapCfg := cfg.Filesystem.MmapConfiguration()
	shouldUseHugeTLB := mmapCfg.HugeTLB.Enabled
	if shouldUseHugeTLB {
		// Make sure the host supports HugeTLB before proceeding with it to prevent
		// excessive log spam.
		shouldUseHugeTLB, err = hostSupportsHugeTLB()
		if err != nil {
			logger.Fatalf("could not determine if host supports HugeTLB: %v", err)
		}
		if !shouldUseHugeTLB {
			logger.Warnf("host doesn't support HugeTLB, proceeding without it")
		}
	}

	policy := cfg.PoolingPolicy
	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		poolOptions(policy.TagEncoderPool, scope.SubScope("tag-encoder-pool")))
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(),
		poolOptions(policy.TagDecoderPool, scope.SubScope("tag-decoder-pool")))
	tagDecoderPool.Init()

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
		SetSeekReaderBufferSize(cfg.Filesystem.SeekReadBufferSize).
		SetMmapEnableHugeTLB(shouldUseHugeTLB).
		SetMmapHugeTLBThreshold(mmapCfg.HugeTLB.Threshold).
		SetRuntimeOptionsManager(runtimeOptsMgr).
		SetTagEncoderPool(tagEncoderPool).
		SetTagDecoderPool(tagDecoderPool)

	var commitLogQueueSize int
	specified := cfg.CommitLog.Queue.Size
	switch cfg.CommitLog.Queue.CalculationType {
	case config.CalculationTypeFixed:
		commitLogQueueSize = specified
	case config.CalculationTypePerCPU:
		commitLogQueueSize = specified * runtime.NumCPU()
	default:
		logger.Fatalf("unknown commit log queue size type: %v",
			cfg.CommitLog.Queue.CalculationType)
	}

	var commitLogQueueChannelSize int
	if cfg.CommitLog.QueueChannel != nil {
		specified := cfg.CommitLog.QueueChannel.Size
		switch cfg.CommitLog.Queue.CalculationType {
		case config.CalculationTypeFixed:
			commitLogQueueChannelSize = specified
		case config.CalculationTypePerCPU:
			commitLogQueueChannelSize = specified * runtime.NumCPU()
		default:
			logger.Fatalf("unknown commit log queue channel size type: %v",
				cfg.CommitLog.Queue.CalculationType)
		}
	} else {
		commitLogQueueChannelSize = int(float64(commitLogQueueSize) / commitlog.MaximumQueueSizeQueueChannelSizeRatio)
	}

	opts = opts.SetCommitLogOptions(opts.CommitLogOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetFilesystemOptions(fsopts).
		SetStrategy(commitlog.StrategyWriteBehind).
		SetFlushSize(cfg.CommitLog.FlushMaxBytes).
		SetFlushInterval(cfg.CommitLog.FlushEvery).
		SetBacklogQueueSize(commitLogQueueSize).
		SetBacklogQueueChannelSize(commitLogQueueChannelSize).
		SetBlockSize(cfg.CommitLog.BlockSize))

	// Set the series cache policy
	seriesCachePolicy := cfg.Cache.SeriesConfiguration().Policy
	opts = opts.SetSeriesCachePolicy(seriesCachePolicy)

	// Apply pooling options
	opts = withEncodingAndPoolingOptions(cfg, logger, opts, cfg.PoolingPolicy)

	// Setup the block retriever
	switch seriesCachePolicy {
	case series.CacheAll:
		// No options needed to be set
	default:
		// All other caching strategies require retrieving series from disk
		// to service a cache miss
		retrieverOpts := fs.NewBlockRetrieverOptions().
			SetBytesPool(opts.BytesPool()).
			SetSegmentReaderPool(opts.SegmentReaderPool()).
			SetIdentifierPool(opts.IdentifierPool())
		if blockRetrieveCfg := cfg.BlockRetrieve; blockRetrieveCfg != nil {
			retrieverOpts = retrieverOpts.
				SetFetchConcurrency(blockRetrieveCfg.FetchConcurrency)
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
	}

	// Set the persistence manager
	pm, err := fs.NewPersistManager(fsopts)
	if err != nil {
		logger.Fatalf("could not create persist manager: %v", err)
	}
	opts = opts.SetPersistManager(pm)

	var (
		envCfg environment.ConfigureResults
	)
	if cfg.EnvironmentConfig.Static == nil {
		logger.Info("creating dynamic config service client with m3cluster")

		envCfg, err = cfg.EnvironmentConfig.Configure(environment.ConfigurationParameters{
			InstrumentOpts: iopts,
			HashingSeed:    cfg.Hashing.Seed,
		})
		if err != nil {
			logger.Fatalf("could not initialize dynamic config: %v", err)
		}
	} else {
		logger.Info("creating static config service client with m3cluster")

		envCfg, err = cfg.EnvironmentConfig.Configure(environment.ConfigurationParameters{
			InstrumentOpts: iopts,
			HostID:         hostID,
		})
		if err != nil {
			logger.Fatalf("could not initialize static config: %v", err)
		}
	}

	if runOpts.ClusterClientCh != nil {
		runOpts.ClusterClientCh <- envCfg.ClusterClient
	}

	opts = opts.SetNamespaceInitializer(envCfg.NamespaceInitializer)

	topo, err := envCfg.TopologyInitializer.Init()
	if err != nil {
		logger.Fatalf("could not initialize m3db topology: %v", err)
	}

	origin := topology.NewHost(hostID, "")
	m3dbClient, err := cfg.Client.NewAdminClient(
		client.ConfigurationParameters{
			InstrumentOptions: iopts.
				SetMetricsScope(iopts.MetricsScope().SubScope("m3dbclient")),
			TopologyInitializer: envCfg.TopologyInitializer,
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetRuntimeOptionsManager(runtimeOptsMgr).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetContextPool(opts.ContextPool()).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetOrigin(origin)
		})
	if err != nil {
		logger.Fatalf("could not create m3db client: %v", err)
	}

	if runOpts.ClientCh != nil {
		runOpts.ClientCh <- m3dbClient
	}

	// Kick off runtime options manager KV watches
	clientAdminOpts := m3dbClient.Options().(client.AdminOptions)
	kvWatchClientConsistencyLevels(envCfg.KVStore, logger,
		clientAdminOpts, runtimeOptsMgr)

	// Set repair options
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
	ttopts := tchannelthrift.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetTagEncoderPool(tagEncoderPool).
		SetTagDecoderPool(tagDecoderPool)

	// Set bootstrap options - We need to create a topology map provider from the
	// same topology that will be passed to the cluster so that when we make
	// bootstrapping decisions they are in sync with the clustered database
	// which is triggering the actual bootstraps. This way, when the clustered
	// database receives a topology update and decides to kick off a bootstrap,
	// the bootstrap process will receaive a topology map that is at least as
	// recent as the one that triggered the bootstrap, if not newer.
	// See GitHub issue #1013 for more details.
	topoMapProvider := newTopoMapProvider(topo)
	bs, err := cfg.Bootstrap.New(opts, topoMapProvider, origin, m3dbClient)
	if err != nil {
		logger.Fatalf("could not create bootstrap process: %v", err)
	}

	opts = opts.SetBootstrapProcessProvider(bs)
	timeout := bootstrapConfigInitTimeout
	kvWatchBootstrappers(envCfg.KVStore, logger, timeout, cfg.Bootstrap.Bootstrappers,
		func(bootstrappers []string) {
			if len(bootstrappers) == 0 {
				logger.Errorf("updated bootstrapper list is empty")
				return
			}

			cfg.Bootstrap.Bootstrappers = bootstrappers
			updated, err := cfg.Bootstrap.New(opts, topoMapProvider, origin, m3dbClient)
			if err != nil {
				logger.Errorf("updated bootstrapper list failed: %v", err)
				return
			}

			bs.SetBootstrapperProvider(updated.BootstrapperProvider())
		})

	// Initialize clustered database
	clusterTopoWatch, err := topo.Watch()
	if err != nil {
		logger.Fatalf("could not create cluster topology watch: %v", err)
	}
	db, err := cluster.NewDatabase(hostID, topo, clusterTopoWatch, opts)
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
		kvWatchNewSeriesLimitPerShard(envCfg.KVStore, logger, topo,
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

func bgValidateProcessLimits(logger xlog.Logger) {
	start := time.Now()
	t := time.NewTicker(bgProcessLimitInterval)
	defer t.Stop()
	for {
		// only monitor for first `maxBgProcessLimitMonitorDuration` of process lifetime
		if time.Since(start) > maxBgProcessLimitMonitorDuration {
			return
		}

		err := validateProcessLimits()
		if err == nil {
			return
		}

		logger.WithFields(
			xlog.NewField("url", xdocs.Path("operational_guide/kernel_configuration")),
		).Warnf(`invalid configuration found [%v], refer to linked documentation for more information`, err)

		<-t.C
	}
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

	err = setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, initClusterLimit)
	if err != nil {
		logger.Warnf("unable to set cluster new series insert limit: %v", err)
	}

	watch, err := store.Watch(kvconfig.ClusterNewSeriesInsertLimitKey)
	if err != nil {
		logger.Errorf("could not watch cluster new series insert limit: %v", err)
		return
	}

	go func() {
		protoValue := &commonpb.Int64Proto{}
		for range watch.C() {
			value := defaultClusterNewSeriesLimit
			if newValue := watch.Get(); newValue != nil {
				if err := newValue.Unmarshal(protoValue); err != nil {
					logger.Warnf("unable to parse new cluster new series insert limit: %v", err)
					continue
				}
				value = int(protoValue.Value)
			}

			err = setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, value)
			if err != nil {
				logger.Warnf("unable to set cluster new series insert limit: %v", err)
				continue
			}
		}
	}()
}

func kvWatchClientConsistencyLevels(
	store kv.Store,
	logger xlog.Logger,
	clientOpts client.AdminOptions,
	runtimeOptsMgr m3dbruntime.OptionsManager,
) {
	setReadConsistencyLevel := func(
		v string,
		applyFn func(topology.ReadConsistencyLevel, m3dbruntime.Options) m3dbruntime.Options,
	) error {
		for _, level := range topology.ValidReadConsistencyLevels() {
			if level.String() == v {
				runtimeOpts := applyFn(level, runtimeOptsMgr.Get())
				return runtimeOptsMgr.Update(runtimeOpts)
			}
		}
		return fmt.Errorf("invalid read consistency level set: %s", v)
	}

	setConsistencyLevel := func(
		v string,
		applyFn func(topology.ConsistencyLevel, m3dbruntime.Options) m3dbruntime.Options,
	) error {
		for _, level := range topology.ValidConsistencyLevels() {
			if level.String() == v {
				runtimeOpts := applyFn(level, runtimeOptsMgr.Get())
				return runtimeOptsMgr.Update(runtimeOpts)
			}
		}
		return fmt.Errorf("invalid consistency level set: %s", v)
	}

	kvWatchStringValue(store, logger,
		kvconfig.ClientBootstrapConsistencyLevel,
		func(value string) error {
			return setReadConsistencyLevel(value,
				func(level topology.ReadConsistencyLevel, opts m3dbruntime.Options) m3dbruntime.Options {
					return opts.SetClientBootstrapConsistencyLevel(level)
				})
		},
		func() error {
			return runtimeOptsMgr.Update(runtimeOptsMgr.Get().
				SetClientBootstrapConsistencyLevel(clientOpts.BootstrapConsistencyLevel()))
		})

	kvWatchStringValue(store, logger,
		kvconfig.ClientReadConsistencyLevel,
		func(value string) error {
			return setReadConsistencyLevel(value,
				func(level topology.ReadConsistencyLevel, opts m3dbruntime.Options) m3dbruntime.Options {
					return opts.SetClientReadConsistencyLevel(level)
				})
		},
		func() error {
			return runtimeOptsMgr.Update(runtimeOptsMgr.Get().
				SetClientReadConsistencyLevel(clientOpts.ReadConsistencyLevel()))
		})

	kvWatchStringValue(store, logger,
		kvconfig.ClientWriteConsistencyLevel,
		func(value string) error {
			return setConsistencyLevel(value,
				func(level topology.ConsistencyLevel, opts m3dbruntime.Options) m3dbruntime.Options {
					return opts.SetClientWriteConsistencyLevel(level)
				})
		},
		func() error {
			return runtimeOptsMgr.Update(runtimeOptsMgr.Get().
				SetClientWriteConsistencyLevel(clientOpts.WriteConsistencyLevel()))
		})
}

func kvWatchStringValue(
	store kv.Store,
	logger xlog.Logger,
	key string,
	onValue func(value string) error,
	onDelete func() error,
) {
	protoValue := &commonpb.StringProto{}

	// First try to eagerly set the value so it doesn't flap if the
	// watch returns but not immediately for an existing value
	value, err := store.Get(key)
	if err != nil && err != kv.ErrNotFound {
		logger.Errorf("could not resolve KV key %s: %v", key, err)
	}
	if err == nil {
		if err := value.Unmarshal(protoValue); err != nil {
			logger.Errorf("could not unmarshal KV key %s: %v", key, err)
		} else if err := onValue(protoValue.Value); err != nil {
			logger.Errorf("could not process value of KV key %s: %v", key, err)
		} else {
			logger.Infof("set KV key %s: %v", key, protoValue.Value)
		}
	}

	watch, err := store.Watch(key)
	if err != nil {
		logger.Errorf("could not watch KV key %s: %v", key, err)
		return
	}

	go func() {
		for range watch.C() {
			newValue := watch.Get()
			if newValue == nil {
				if err := onDelete(); err != nil {
					logger.Warnf("could not set default for KV key %s: %v", key, err)
				}
				continue
			}

			err := newValue.Unmarshal(protoValue)
			if err != nil {
				logger.Warnf("could not unmarshal KV key %s: %v", key, err)
				continue
			}
			if err := onValue(protoValue.Value); err != nil {
				logger.Warnf("could not process change for KV key %s: %v", key, err)
				continue
			}
			logger.Infof("set KV key %s: %v", key, protoValue.Value)
		}
	}()
}

func setNewSeriesLimitPerShardOnChange(
	topo topology.Topology,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	clusterLimit int,
) error {
	perPlacedShardLimit := clusterLimitToPlacedShardLimit(topo, clusterLimit)
	runtimeOpts := runtimeOptsMgr.Get()
	if runtimeOpts.WriteNewSeriesLimitPerShardPerSecond() == perPlacedShardLimit {
		// Not changed, no need to set the value and trigger a runtime options update
		return nil
	}

	newRuntimeOpts := runtimeOpts.
		SetWriteNewSeriesLimitPerShardPerSecond(perPlacedShardLimit)
	return runtimeOptsMgr.Update(newRuntimeOpts)
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
	cfg config.DBConfiguration,
	logger xlog.Logger,
	opts storage.Options,
	policy config.PoolingPolicy,
) storage.Options {
	iopts := opts.InstrumentOptions()
	scope := opts.InstrumentOptions().MetricsScope()

	bytesPoolOpts := pool.NewObjectPoolOptions().
		SetInstrumentOptions(iopts.SetMetricsScope(scope.SubScope("bytes-pool")))
	checkedBytesPoolOpts := bytesPoolOpts.
		SetInstrumentOptions(iopts.SetMetricsScope(scope.SubScope("checked-bytes-pool")))
	buckets := make([]pool.Bucket, len(policy.BytesPool.Buckets))
	for i, bucket := range policy.BytesPool.Buckets {
		var b pool.Bucket
		b.Capacity = bucket.Capacity
		b.Count = bucket.Size
		b.Options = bytesPoolOpts.
			SetRefillLowWatermark(bucket.RefillLowWaterMark).
			SetRefillHighWatermark(bucket.RefillHighWaterMark)
		buckets[i] = b
		logger.Infof("bytes pool registering bucket capacity=%d, size=%d, "+
			"refillLowWatermark=%f, refillHighWatermark=%f",
			bucket.Capacity, bucket.Size,
			bucket.RefillLowWaterMark, bucket.RefillHighWaterMark)
	}

	var bytesPool pool.CheckedBytesPool
	switch policy.Type {
	case config.SimplePooling:
		bytesPool = pool.NewCheckedBytesPool(
			buckets,
			checkedBytesPoolOpts,
			func(s []pool.Bucket) pool.BytesPool {
				return pool.NewBytesPool(s, bytesPoolOpts)
			})
	default:
		logger.Fatalf("unrecognized pooling type: %s", policy.Type)
	}

	logger.Infof("bytes pool %s init", policy.Type)
	bytesPool.Init()

	segmentReaderPool := xio.NewSegmentReaderPool(
		poolOptions(policy.SegmentReaderPool, scope.SubScope("segment-reader-pool")))
	segmentReaderPool.Init()
	encoderPool := encoding.NewEncoderPool(
		poolOptions(policy.EncoderPool, scope.SubScope("encoder-pool")))
	closersPoolOpts := poolOptions(policy.ClosersPool, scope.SubScope("closers-pool"))
	contextPoolOpts := poolOptions(policy.ContextPool.PoolPolicy(), scope.SubScope("context-pool"))
	contextPool := context.NewPool(context.NewOptions().
		SetContextPoolOptions(contextPoolOpts).
		SetFinalizerPoolOptions(closersPoolOpts).
		SetMaxPooledFinalizerCapacity(policy.ContextPool.MaxFinalizerCapacityWithDefault()))
	iteratorPool := encoding.NewReaderIteratorPool(
		poolOptions(policy.IteratorPool, scope.SubScope("iterator-pool")))
	multiIteratorPool := encoding.NewMultiReaderIteratorPool(
		poolOptions(policy.IteratorPool, scope.SubScope("multi-iterator-pool")))

	var writeBatchPoolInitialBatchSize *int
	if policy.WriteBatchPool.InitialBatchSize != nil {
		writeBatchPoolInitialBatchSize = policy.WriteBatchPool.InitialBatchSize
	}
	var writeBatchPoolMaxBatchSize *int
	if policy.WriteBatchPool.MaxBatchSize != nil {
		writeBatchPoolMaxBatchSize = policy.WriteBatchPool.MaxBatchSize
	}
	writeBatchPool := ts.NewWriteBatchPool(
		poolOptions(policy.WriteBatchPool.Pool, scope.SubScope("write-batch-pool")),
		writeBatchPoolInitialBatchSize,
		writeBatchPoolMaxBatchSize)

	identifierPool := ident.NewPool(bytesPool, ident.PoolOptions{
		IDPoolOptions:           poolOptions(policy.IdentifierPool, scope.SubScope("identifier-pool")),
		TagsPoolOptions:         maxCapacityPoolOptions(policy.TagsPool, scope.SubScope("tags-pool")),
		TagsCapacity:            policy.TagsPool.Capacity,
		TagsMaxCapacity:         policy.TagsPool.MaxCapacity,
		TagsIteratorPoolOptions: poolOptions(policy.TagsIteratorPool, scope.SubScope("tags-iterator-pool")),
	})

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

	writeBatchPool.Init()

	opts = opts.
		SetBytesPool(bytesPool).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
		SetIdentifierPool(identifierPool).
		SetFetchBlockMetadataResultsPool(fetchBlockMetadataResultsPool).
		SetFetchBlocksMetadataResultsPool(fetchBlocksMetadataResultsPool).
		SetWriteBatchPool(writeBatchPool)

	blockOpts := opts.DatabaseBlockOptions().
		SetDatabaseBlockAllocSize(policy.BlockAllocSize).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetSegmentReaderPool(segmentReaderPool).
		SetBytesPool(bytesPool)

	if opts.SeriesCachePolicy() == series.CacheLRU {
		var (
			runtimeOpts   = opts.RuntimeOptionsManager()
			wiredListOpts = block.WiredListOptions{
				RuntimeOptionsManager: runtimeOpts,
				InstrumentOptions:     iopts,
				ClockOptions:          opts.ClockOptions(),
			}
			lruCfg = cfg.Cache.SeriesConfiguration().LRU
		)

		if lruCfg != nil && lruCfg.EventsChannelSize > 0 {
			wiredListOpts.EventsChannelSize = int(lruCfg.EventsChannelSize)
		}
		wiredList := block.NewWiredList(wiredListOpts)
		blockOpts = blockOpts.SetWiredList(wiredList)
	}
	blockPool := block.NewDatabaseBlockPool(poolOptions(policy.BlockPool,
		scope.SubScope("block-pool")))
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts)
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
		SetBytesPool(bytesPool).
		SetIdentifierPool(identifierPool))

	resultsPool := index.NewResultsPool(poolOptions(policy.IndexResultsPool,
		scope.SubScope("index-results-pool")))
	indexOpts := opts.IndexOptions().
		SetInstrumentOptions(iopts).
		SetMemSegmentOptions(
			opts.IndexOptions().MemSegmentOptions().SetInstrumentOptions(iopts)).
		SetFSTSegmentOptions(
			opts.IndexOptions().FSTSegmentOptions().SetInstrumentOptions(iopts)).
		SetIdentifierPool(identifierPool).
		SetCheckedBytesPool(bytesPool).
		SetResultsPool(resultsPool)
	resultsPool.Init(func() index.Results { return index.NewResults(indexOpts) })

	return opts.SetIndexOptions(indexOpts)
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

func maxCapacityPoolOptions(
	policy config.MaxCapacityPoolPolicy,
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

func hostSupportsHugeTLB() (bool, error) {
	// Try and determine if the host supports HugeTLB in the first place
	withHugeTLB, err := mmap.Bytes(10, mmap.Options{
		HugeTLB: mmap.HugeTLBOptions{
			Enabled:   true,
			Threshold: 0,
		},
	})
	if err != nil {
		return false, fmt.Errorf("could not mmap anonymous region: %v", err)
	}
	defer mmap.Munmap(withHugeTLB.Result)

	if withHugeTLB.Warning == nil {
		// If there was no warning, then the host didn't complain about
		// usa of huge TLB
		return true, nil
	}

	// If we got a warning, try mmap'ing without HugeTLB
	withoutHugeTLB, err := mmap.Bytes(10, mmap.Options{})
	if err != nil {
		return false, fmt.Errorf("could not mmap anonymous region: %v", err)
	}
	defer mmap.Munmap(withoutHugeTLB.Result)
	if withoutHugeTLB.Warning == nil {
		// The machine doesn't support HugeTLB, proceed without it
		return false, nil
	}
	// The warning was probably caused by something else, proceed using HugeTLB
	return true, nil
}

func newTopoMapProvider(t topology.Topology) *topoMapProvider {
	return &topoMapProvider{t}
}

type topoMapProvider struct {
	t topology.Topology
}

func (t *topoMapProvider) TopologyMap() (topology.Map, error) {
	if t.t == nil {
		return nil, errors.New("topology map provider has not be set yet")
	}

	return t.t.Get(), nil
}
