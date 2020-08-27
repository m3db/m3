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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	queryconfig "github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/dbnode/namespace"
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
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/cluster"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/storage/stats"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	xtchannel "github.com/m3db/m3/src/dbnode/x/tchannel"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/query/api/v1/handler/placement"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	xconfig "github.com/m3db/m3/src/x/config"
	xcontext "github.com/m3db/m3/src/x/context"
	xdebug "github.com/m3db/m3/src/x/debug"
	xdocs "github.com/m3db/m3/src/x/docs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/lockfile"
	"github.com/m3db/m3/src/x/mmap"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

const (
	bootstrapConfigInitTimeout       = 10 * time.Second
	serverGracefulCloseTimeout       = 10 * time.Second
	bgProcessLimitInterval           = 10 * time.Second
	maxBgProcessLimitMonitorDuration = 5 * time.Minute
	cpuProfileDuration               = 5 * time.Second
	filePathPrefixLockFile           = ".lock"
	defaultServiceName               = "m3dbnode"
	skipRaiseProcessLimitsEnvVar     = "SKIP_PROCESS_LIMITS_RAISE"
	skipRaiseProcessLimitsEnvVarTrue = "true"
	mmapReporterMetricName           = "mmap-mapped-bytes"
	mmapReporterTagName              = "map-name"
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

	// QueryStatsTrackerFn returns a tracker for tracking query stats.
	QueryStatsTrackerFn func(instrument.Options, stats.QueryStatsOptions) stats.QueryStatsTracker

	// CustomOptions are custom options to apply to the session.
	CustomOptions []client.CustomAdminOption

	// StorageOptions are options to apply to the database storage options.
	StorageOptions StorageOptions
}

// Run runs the server programmatically given a filename for the
// configuration file.
func Run(runOpts RunOptions) {
	var cfg config.DBConfiguration
	if runOpts.ConfigFile != "" {
		var rootCfg config.Configuration
		if err := xconfig.LoadFile(&rootCfg, runOpts.ConfigFile, xconfig.Options{}); err != nil {
			// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
			// sending stdlib "log" to black hole. Don't remove unless with good reason.
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
			os.Exit(1)
		}

		cfg = *rootCfg.DB
	} else {
		cfg = runOpts.Config
	}

	err := cfg.InitDefaultsAndValidate()
	if err != nil {
		// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
		// sending stdlib "log" to black hole. Don't remove unless with good reason.
		fmt.Fprintf(os.Stderr, "error initializing config defaults and validating config: %v", err)
		os.Exit(1)
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
		// sending stdlib "log" to black hole. Don't remove unless with good reason.
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}
	defer logger.Sync()

	xconfig.WarnOnDeprecation(cfg, logger)

	// By default attempt to raise process limits, which is a benign operation.
	skipRaiseLimits := strings.TrimSpace(os.Getenv(skipRaiseProcessLimitsEnvVar))
	if skipRaiseLimits != skipRaiseProcessLimitsEnvVarTrue {
		// Raise fd limits to nr_open system limit
		result, err := xos.RaiseProcessNoFileToNROpen()
		if err != nil {
			logger.Warn("unable to raise rlimit", zap.Error(err))
		} else {
			logger.Info("raised rlimit no file fds limit",
				zap.Bool("required", result.RaisePerformed),
				zap.Uint64("sysNROpenValue", result.NROpenValue),
				zap.Uint64("noFileMaxValue", result.NoFileMaxValue),
				zap.Uint64("noFileCurrValue", result.NoFileCurrValue))
		}
	}

	// Parse file and directory modes
	newFileMode, err := cfg.Filesystem.ParseNewFileMode()
	if err != nil {
		logger.Fatal("could not parse new file mode", zap.Error(err))
	}

	newDirectoryMode, err := cfg.Filesystem.ParseNewDirectoryMode()
	if err != nil {
		logger.Fatal("could not parse new directory mode", zap.Error(err))
	}

	// Obtain a lock on `filePathPrefix`, or exit if another process already has it.
	// The lock consists of a lock file (on the file system) and a lock in memory.
	// When the process exits gracefully, both the lock file and the lock will be removed.
	// If the process exits ungracefully, only the lock in memory will be removed, the lock
	// file will remain on the file system. When a dbnode starts after an ungracefully stop,
	// it will be able to acquire the lock despite the fact the the lock file exists.
	lockPath := path.Join(cfg.Filesystem.FilePathPrefixOrDefault(), filePathPrefixLockFile)
	fslock, err := lockfile.CreateAndAcquire(lockPath, newDirectoryMode)
	if err != nil {
		logger.Fatal("could not acquire lock", zap.String("path", lockPath), zap.Error(err))
	}
	defer fslock.Release()

	go bgValidateProcessLimits(logger)
	debug.SetGCPercent(cfg.GCPercentage)

	scope, _, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Error(err))
	}

	hostID, err := cfg.HostID.Resolve()
	if err != nil {
		logger.Fatal("could not resolve local host ID", zap.Error(err))
	}

	var (
		tracer      opentracing.Tracer
		traceCloser io.Closer
	)

	if cfg.Tracing == nil {
		tracer = opentracing.NoopTracer{}
		logger.Info("tracing disabled; set `tracing.backend` to enable")
	} else {
		// setup tracer
		serviceName := cfg.Tracing.ServiceName
		if serviceName == "" {
			serviceName = defaultServiceName
		}
		tracer, traceCloser, err = cfg.Tracing.NewTracer(serviceName, scope.SubScope("jaeger"), logger)
		if err != nil {
			tracer = opentracing.NoopTracer{}
			logger.Warn("could not initialize tracing; using no-op tracer instead",
				zap.String("service", serviceName), zap.Error(err))
		} else {
			defer traceCloser.Close()
			logger.Info("tracing enabled", zap.String("service", serviceName))
		}
	}

	// Presence of KV server config indicates embedded etcd cluster
	if cfg.EnvironmentConfig.SeedNodes == nil {
		logger.Info("no seed nodes set, using dedicated etcd cluster")
	} else {
		// Default etcd client clusters if not set already
		service, err := cfg.EnvironmentConfig.Services.SyncCluster()
		if err != nil {
			logger.Fatal("invalid cluster configuration", zap.Error(err))
		}

		clusters := service.Service.ETCDClusters
		seedNodes := cfg.EnvironmentConfig.SeedNodes.InitialCluster
		if len(clusters) == 0 {
			endpoints, err := config.InitialClusterEndpoints(seedNodes)
			if err != nil {
				logger.Fatal("unable to create etcd clusters", zap.Error(err))
			}

			zone := service.Service.Zone

			logger.Info("using seed nodes etcd cluster",
				zap.String("zone", zone), zap.Strings("endpoints", endpoints))
			service.Service.ETCDClusters = []etcd.ClusterConfig{{
				Zone:      zone,
				Endpoints: endpoints,
			}}
		}

		seedNodeHostIDs := make([]string, 0, len(seedNodes))
		for _, entry := range seedNodes {
			seedNodeHostIDs = append(seedNodeHostIDs, entry.HostID)
		}
		logger.Info("resolving seed node configuration",
			zap.String("hostID", hostID), zap.Strings("seedNodeHostIDs", seedNodeHostIDs),
		)

		if !config.IsSeedNode(seedNodes, hostID) {
			logger.Info("not a seed node, using cluster seed nodes")
		} else {
			logger.Info("seed node, starting etcd server")

			etcdCfg, err := config.NewEtcdEmbedConfig(cfg)
			if err != nil {
				logger.Fatal("unable to create etcd config", zap.Error(err))
			}

			e, err := embed.StartEtcd(etcdCfg)
			if err != nil {
				logger.Fatal("could not start embedded etcd", zap.Error(err))
			}

			if runOpts.EmbeddedKVCh != nil {
				// Notify on embedded KV bootstrap chan if specified
				runOpts.EmbeddedKVCh <- struct{}{}
			}

			defer e.Close()
		}
	}

	// By default use histogram timers for timers that
	// are constructed allowing for type to be picked
	// by the caller using instrument.NewTimer(...).
	timerOpts := instrument.NewHistogramTimerOptions(instrument.HistogramTimerOptions{})
	timerOpts.StandardSampleRate = cfg.Metrics.SampleRate()

	var (
		opts  = storage.NewOptions()
		iopts = opts.InstrumentOptions().
			SetLogger(logger).
			SetMetricsScope(scope).
			SetTimerOptions(timerOpts).
			SetTracer(tracer)
	)
	opts = opts.SetInstrumentOptions(iopts)

	// Only override the default MemoryTracker (which has default limits) if a custom limit has
	// been set.
	if cfg.Limits.MaxOutstandingRepairedBytes > 0 {
		memTrackerOptions := storage.NewMemoryTrackerOptions(cfg.Limits.MaxOutstandingRepairedBytes)
		memTracker := storage.NewMemoryTracker(memTrackerOptions)
		opts = opts.SetMemoryTracker(memTracker)
	}

	opentracing.SetGlobalTracer(tracer)

	if cfg.Index.MaxQueryIDsConcurrency != 0 {
		queryIDsWorkerPool := xsync.NewWorkerPool(cfg.Index.MaxQueryIDsConcurrency)
		queryIDsWorkerPool.Init()
		opts = opts.SetQueryIDsWorkerPool(queryIDsWorkerPool)
	} else {
		logger.Warn("max index query IDs concurrency was not set, falling back to default value")
	}

	buildReporter := instrument.NewBuildReporter(iopts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatal("unable to start build reporter", zap.Error(err))
	}
	defer buildReporter.Stop()

	mmapCfg := cfg.Filesystem.MmapConfigurationOrDefault()
	shouldUseHugeTLB := mmapCfg.HugeTLB.Enabled
	if shouldUseHugeTLB {
		// Make sure the host supports HugeTLB before proceeding with it to prevent
		// excessive log spam.
		shouldUseHugeTLB, err = hostSupportsHugeTLB()
		if err != nil {
			logger.Fatal("could not determine if host supports HugeTLB", zap.Error(err))
		}
		if !shouldUseHugeTLB {
			logger.Warn("host doesn't support HugeTLB, proceeding without it")
		}
	}

	mmapReporter := newMmapReporter(scope)
	mmapReporterCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mmapReporter.Run(mmapReporterCtx)
	opts = opts.SetMmapReporter(mmapReporter)

	runtimeOpts := m3dbruntime.NewOptions().
		SetPersistRateLimitOptions(ratelimit.NewOptions().
			SetLimitEnabled(true).
			SetLimitMbps(cfg.Filesystem.ThroughputLimitMbpsOrDefault()).
			SetLimitCheckEvery(cfg.Filesystem.ThroughputCheckEveryOrDefault())).
		SetWriteNewSeriesAsync(cfg.WriteNewSeriesAsync).
		SetWriteNewSeriesBackoffDuration(cfg.WriteNewSeriesBackoffDuration)
	if lruCfg := cfg.Cache.SeriesConfiguration().LRU; lruCfg != nil {
		runtimeOpts = runtimeOpts.SetMaxWiredBlocks(lruCfg.MaxBlocks)
	}

	// Setup postings list cache.
	var (
		plCacheConfig  = cfg.Cache.PostingsListConfiguration()
		plCacheSize    = plCacheConfig.SizeOrDefault()
		plCacheOptions = index.PostingsListCacheOptions{
			InstrumentOptions: opts.InstrumentOptions().
				SetMetricsScope(scope.SubScope("postings-list-cache")),
		}
	)
	postingsListCache, stopReporting, err := index.NewPostingsListCache(plCacheSize, plCacheOptions)
	if err != nil {
		logger.Fatal("could not construct postings list cache", zap.Error(err))
	}
	defer stopReporting()

	// Setup query stats tracking.
	statsOpts := stats.QueryStatsOptions{
		Lookback: stats.DefaultLookback,
	}
	if max := runOpts.Config.Limits.MaxRecentlyQueriedSeriesBlocks; max != nil {
		statsOpts = stats.QueryStatsOptions{
			MaxDocs:  max.Value,
			Lookback: max.Lookback,
		}
	}
	if err := statsOpts.Validate(); err != nil {
		logger.Fatal("could not construct query stats options from config", zap.Error(err))
	}

	tracker := stats.DefaultQueryStatsTracker(iopts, statsOpts)
	if runOpts.QueryStatsTrackerFn != nil {
		tracker = runOpts.QueryStatsTrackerFn(iopts, statsOpts)
	}

	queryStats := stats.NewQueryStats(tracker)
	queryStats.Start()
	defer queryStats.Stop()

	// FOLLOWUP(prateek): remove this once we have the runtime options<->index wiring done
	indexOpts := opts.IndexOptions()
	insertMode := index.InsertSync
	if cfg.WriteNewSeriesAsync {
		insertMode = index.InsertAsync
	}
	indexOpts = indexOpts.SetInsertMode(insertMode).
		SetPostingsListCache(postingsListCache).
		SetReadThroughSegmentOptions(index.ReadThroughSegmentOptions{
			CacheRegexp: plCacheConfig.CacheRegexpOrDefault(),
			CacheTerms:  plCacheConfig.CacheTermsOrDefault(),
		}).
		SetMmapReporter(mmapReporter).
		SetQueryStats(queryStats)
	opts = opts.SetIndexOptions(indexOpts)

	if tick := cfg.Tick; tick != nil {
		runtimeOpts = runtimeOpts.
			SetTickSeriesBatchSize(tick.SeriesBatchSize).
			SetTickPerSeriesSleepDuration(tick.PerSeriesSleepDuration).
			SetTickMinimumInterval(tick.MinimumInterval)
	}

	runtimeOptsMgr := m3dbruntime.NewOptionsManager()
	if err := runtimeOptsMgr.Update(runtimeOpts); err != nil {
		logger.Fatal("could not set initial runtime options", zap.Error(err))
	}
	defer runtimeOptsMgr.Close()

	opts = opts.SetRuntimeOptionsManager(runtimeOptsMgr)

	policy := cfg.PoolingPolicy
	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		poolOptions(
			policy.TagEncoderPool,
			scope.SubScope("tag-encoder-pool")))
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		poolOptions(
			policy.TagDecoderPool,
			scope.SubScope("tag-decoder-pool")))
	tagDecoderPool.Init()

	// Pass nil for block.LeaseVerifier for now and it will be set after the
	// db is constructed (since the db is required to construct a
	// block.LeaseVerifier). Initialized here because it needs to be propagated
	// to both the DB and the blockRetriever.
	blockLeaseManager := block.NewLeaseManager(nil)
	opts = opts.SetBlockLeaseManager(blockLeaseManager)
	fsopts := fs.NewOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions().
			SetMetricsScope(scope.SubScope("database.fs"))).
		SetFilePathPrefix(cfg.Filesystem.FilePathPrefixOrDefault()).
		SetNewFileMode(newFileMode).
		SetNewDirectoryMode(newDirectoryMode).
		SetWriterBufferSize(cfg.Filesystem.WriteBufferSizeOrDefault()).
		SetDataReaderBufferSize(cfg.Filesystem.DataReadBufferSizeOrDefault()).
		SetInfoReaderBufferSize(cfg.Filesystem.InfoReadBufferSizeOrDefault()).
		SetSeekReaderBufferSize(cfg.Filesystem.SeekReadBufferSizeOrDefault()).
		SetMmapEnableHugeTLB(shouldUseHugeTLB).
		SetMmapHugeTLBThreshold(mmapCfg.HugeTLB.Threshold).
		SetRuntimeOptionsManager(runtimeOptsMgr).
		SetTagEncoderPool(tagEncoderPool).
		SetTagDecoderPool(tagDecoderPool).
		SetForceIndexSummariesMmapMemory(cfg.Filesystem.ForceIndexSummariesMmapMemoryOrDefault()).
		SetForceBloomFilterMmapMemory(cfg.Filesystem.ForceBloomFilterMmapMemoryOrDefault()).
		SetIndexBloomFilterFalsePositivePercent(cfg.Filesystem.BloomFilterFalsePositivePercentOrDefault()).
		SetMmapReporter(mmapReporter)

	var commitLogQueueSize int
	specified := cfg.CommitLog.Queue.Size
	switch cfg.CommitLog.Queue.CalculationType {
	case config.CalculationTypeFixed:
		commitLogQueueSize = specified
	case config.CalculationTypePerCPU:
		commitLogQueueSize = specified * runtime.NumCPU()
	default:
		logger.Fatal("unknown commit log queue size type",
			zap.Any("type", cfg.CommitLog.Queue.CalculationType))
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
			logger.Fatal("unknown commit log queue channel size type",
				zap.Any("type", cfg.CommitLog.Queue.CalculationType))
		}
	} else {
		commitLogQueueChannelSize = int(float64(commitLogQueueSize) / commitlog.MaximumQueueSizeQueueChannelSizeRatio)
	}

	// Set the series cache policy.
	seriesCachePolicy := cfg.Cache.SeriesConfiguration().Policy
	opts = opts.SetSeriesCachePolicy(seriesCachePolicy)

	// Apply pooling options.
	opts = withEncodingAndPoolingOptions(cfg, logger, opts, cfg.PoolingPolicy)

	opts = opts.SetCommitLogOptions(opts.CommitLogOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetFilesystemOptions(fsopts).
		SetStrategy(commitlog.StrategyWriteBehind).
		SetFlushSize(cfg.CommitLog.FlushMaxBytes).
		SetFlushInterval(cfg.CommitLog.FlushEvery).
		SetBacklogQueueSize(commitLogQueueSize).
		SetBacklogQueueChannelSize(commitLogQueueChannelSize))

	// Setup the block retriever
	switch seriesCachePolicy {
	case series.CacheAll:
		// No options needed to be set
	default:
		// All other caching strategies require retrieving series from disk
		// to service a cache miss
		retrieverOpts := fs.NewBlockRetrieverOptions().
			SetBytesPool(opts.BytesPool()).
			SetRetrieveRequestPool(opts.RetrieveRequestPool()).
			SetIdentifierPool(opts.IdentifierPool()).
			SetBlockLeaseManager(blockLeaseManager)
		if blockRetrieveCfg := cfg.BlockRetrieve; blockRetrieveCfg != nil {
			retrieverOpts = retrieverOpts.
				SetFetchConcurrency(blockRetrieveCfg.FetchConcurrency)
		}
		blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
			func(md namespace.Metadata, shardSet sharding.ShardSet) (block.DatabaseBlockRetriever, error) {
				retriever, err := fs.NewBlockRetriever(retrieverOpts, fsopts)
				if err != nil {
					return nil, err
				}
				if err := retriever.Open(md, shardSet); err != nil {
					return nil, err
				}
				return retriever, nil
			})
		opts = opts.SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
	}

	// Set the persistence manager
	pm, err := fs.NewPersistManager(fsopts)
	if err != nil {
		logger.Fatal("could not create persist manager", zap.Error(err))
	}
	opts = opts.SetPersistManager(pm)

	var (
		envCfg environment.ConfigureResults
	)
	if len(cfg.EnvironmentConfig.Statics) == 0 {
		logger.Info("creating dynamic config service client with m3cluster")

		envCfg, err = cfg.EnvironmentConfig.Configure(environment.ConfigurationParameters{
			InstrumentOpts:         iopts,
			HashingSeed:            cfg.Hashing.Seed,
			NewDirectoryMode:       newDirectoryMode,
			ForceColdWritesEnabled: runOpts.StorageOptions.ForceColdWritesEnabled,
		})
		if err != nil {
			logger.Fatal("could not initialize dynamic config", zap.Error(err))
		}
	} else {
		logger.Info("creating static config service client with m3cluster")

		envCfg, err = cfg.EnvironmentConfig.Configure(environment.ConfigurationParameters{
			InstrumentOpts:         iopts,
			HostID:                 hostID,
			ForceColdWritesEnabled: runOpts.StorageOptions.ForceColdWritesEnabled,
		})
		if err != nil {
			logger.Fatal("could not initialize static config", zap.Error(err))
		}
	}

	syncCfg, err := envCfg.SyncCluster()
	if err != nil {
		logger.Fatal("invalid cluster config", zap.Error(err))
	}
	if runOpts.ClusterClientCh != nil {
		runOpts.ClusterClientCh <- syncCfg.ClusterClient
	}

	opts = opts.SetNamespaceInitializer(syncCfg.NamespaceInitializer)

	// Set tchannelthrift options.
	ttopts := tchannelthrift.NewOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetTopologyInitializer(syncCfg.TopologyInitializer).
		SetIdentifierPool(opts.IdentifierPool()).
		SetTagEncoderPool(tagEncoderPool).
		SetTagDecoderPool(tagDecoderPool).
		SetCheckedBytesWrapperPool(opts.CheckedBytesWrapperPool()).
		SetMaxOutstandingWriteRequests(cfg.Limits.MaxOutstandingWriteRequests).
		SetMaxOutstandingReadRequests(cfg.Limits.MaxOutstandingReadRequests)

	// Start servers before constructing the DB so orchestration tools can check health endpoints
	// before topology is set.
	var (
		contextPool  = opts.ContextPool()
		tchannelOpts = xtchannel.NewDefaultChannelOptions()
		// Pass nil for the database argument because we haven't constructed it yet. We'll call
		// SetDatabase() once we've initialized it.
		service = ttnode.NewService(nil, ttopts)
	)
	if cfg.TChannel != nil {
		tchannelOpts.MaxIdleTime = cfg.TChannel.MaxIdleTime
		tchannelOpts.IdleCheckInterval = cfg.TChannel.IdleCheckInterval
	}
	tchanOpts := ttnode.NewOptions(tchannelOpts).
		SetInstrumentOptions(opts.InstrumentOptions())
	if fn := runOpts.StorageOptions.TChanNodeServerFn; fn != nil {
		tchanOpts = tchanOpts.SetTChanNodeServerFn(fn)
	}
	tchannelthriftNodeClose, err := ttnode.NewServer(service,
		cfg.ListenAddress, contextPool, tchanOpts).ListenAndServe()
	if err != nil {
		logger.Fatal("could not open tchannelthrift interface",
			zap.String("address", cfg.ListenAddress), zap.Error(err))
	}
	defer tchannelthriftNodeClose()
	logger.Info("node tchannelthrift: listening", zap.String("address", cfg.ListenAddress))

	httpjsonNodeClose, err := hjnode.NewServer(service,
		cfg.HTTPNodeListenAddress, contextPool, nil).ListenAndServe()
	if err != nil {
		logger.Fatal("could not open httpjson interface",
			zap.String("address", cfg.HTTPNodeListenAddress), zap.Error(err))
	}
	defer httpjsonNodeClose()
	logger.Info("node httpjson: listening", zap.String("address", cfg.HTTPNodeListenAddress))

	if cfg.DebugListenAddress != "" {
		var debugWriter xdebug.ZipWriter
		handlerOpts, err := placement.NewHandlerOptions(syncCfg.ClusterClient,
			queryconfig.Configuration{}, nil, iopts)
		if err != nil {
			logger.Warn("could not create handler options for debug writer", zap.Error(err))
		} else {
			envCfg, err := cfg.EnvironmentConfig.Services.SyncCluster()
			if err != nil || envCfg.Service == nil {
				logger.Warn("could not get cluster config for debug writer",
					zap.Error(err),
					zap.Bool("envCfgServiceIsNil", envCfg.Service == nil))
			} else {
				debugWriter, err = xdebug.NewPlacementAndNamespaceZipWriterWithDefaultSources(
					cpuProfileDuration,
					syncCfg.ClusterClient,
					handlerOpts,
					[]handleroptions.ServiceNameAndDefaults{
						{
							ServiceName: handleroptions.M3DBServiceName,
							Defaults: []handleroptions.ServiceOptionsDefault{
								handleroptions.WithDefaultServiceEnvironment(envCfg.Service.Env),
								handleroptions.WithDefaultServiceZone(envCfg.Service.Zone),
							},
						},
					},
					iopts)
				if err != nil {
					logger.Error("unable to create debug writer", zap.Error(err))
				}
			}
		}

		go func() {
			mux := http.DefaultServeMux
			if debugWriter != nil {
				if err := debugWriter.RegisterHandler(xdebug.DebugURL, mux); err != nil {
					logger.Error("unable to register debug writer endpoint", zap.Error(err))
				}
			}

			if err := http.ListenAndServe(cfg.DebugListenAddress, mux); err != nil {
				logger.Error("debug server could not listen",
					zap.String("address", cfg.DebugListenAddress), zap.Error(err))
			} else {
				logger.Info("debug server listening",
					zap.String("address", cfg.DebugListenAddress),
				)
			}
		}()
	}

	topo, err := syncCfg.TopologyInitializer.Init()
	if err != nil {
		logger.Fatal("could not initialize m3db topology", zap.Error(err))
	}

	var protoEnabled bool
	if cfg.Proto != nil && cfg.Proto.Enabled {
		protoEnabled = true
	}
	schemaRegistry := namespace.NewSchemaRegistry(protoEnabled, logger)
	// For application m3db client integration test convenience (where a local dbnode is started as a docker container),
	// we allow loading user schema from local file into schema registry.
	if protoEnabled {
		for nsID, protoConfig := range cfg.Proto.SchemaRegistry {
			dummyDeployID := "fromconfig"
			if err := namespace.LoadSchemaRegistryFromFile(schemaRegistry, ident.StringID(nsID),
				dummyDeployID,
				protoConfig.SchemaFilePath, protoConfig.MessageName); err != nil {
				logger.Fatal("could not load schema from configuration", zap.Error(err))
			}
		}
	}

	origin := topology.NewHost(hostID, "")
	m3dbClient, err := newAdminClient(
		cfg.Client, iopts, tchannelOpts, syncCfg.TopologyInitializer,
		runtimeOptsMgr, origin, protoEnabled, schemaRegistry,
		syncCfg.KVStore, logger, runOpts.CustomOptions)

	if err != nil {
		logger.Fatal("could not create m3db client", zap.Error(err))
	}

	if runOpts.ClientCh != nil {
		runOpts.ClientCh <- m3dbClient
	}

	documentsBuilderAlloc := index.NewBootstrapResultDocumentsBuilderAllocator(
		opts.IndexOptions())
	rsOpts := result.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetSeriesCachePolicy(opts.SeriesCachePolicy()).
		SetIndexDocumentsBuilderAllocator(documentsBuilderAlloc)

	var repairClients []client.AdminClient
	if cfg.Repair != nil && cfg.Repair.Enabled {
		repairClients = append(repairClients, m3dbClient)
	}
	if cfg.Replication != nil {
		for _, cluster := range cfg.Replication.Clusters {
			if !cluster.RepairEnabled {
				continue
			}

			// Pass nil for the topology initializer because we want to create
			// a new one for the cluster we wish to replicate from, not use the
			// same one as the cluster this node belongs to.
			var topologyInitializer topology.Initializer
			// Guaranteed to not be nil if repair is enabled by config validation.
			clientCfg := *cluster.Client
			clusterClient, err := newAdminClient(
				clientCfg, iopts, tchannelOpts, topologyInitializer,
				runtimeOptsMgr, origin, protoEnabled, schemaRegistry,
				syncCfg.KVStore, logger, runOpts.CustomOptions)
			if err != nil {
				logger.Fatal(
					"unable to create client for replicated cluster",
					zap.String("clusterName", cluster.Name), zap.Error(err))
			}
			repairClients = append(repairClients, clusterClient)
		}
	}
	repairEnabled := len(repairClients) > 0
	if repairEnabled {
		repairOpts := opts.RepairOptions().
			SetAdminClients(repairClients)

		if cfg.Repair != nil {
			repairOpts = repairOpts.
				SetResultOptions(rsOpts).
				SetDebugShadowComparisonsEnabled(cfg.Repair.DebugShadowComparisonsEnabled)
			if cfg.Repair.Throttle > 0 {
				repairOpts = repairOpts.SetRepairThrottle(cfg.Repair.Throttle)
			}
			if cfg.Repair.CheckInterval > 0 {
				repairOpts = repairOpts.SetRepairCheckInterval(cfg.Repair.CheckInterval)
			}

			if cfg.Repair.DebugShadowComparisonsPercentage > 0 {
				// Set conditionally to avoid stomping on the default value of 1.0.
				repairOpts = repairOpts.SetDebugShadowComparisonsPercentage(cfg.Repair.DebugShadowComparisonsPercentage)
			}
		}

		opts = opts.
			SetRepairEnabled(true).
			SetRepairOptions(repairOpts)
	} else {
		opts = opts.SetRepairEnabled(false)
	}

	if runOpts.StorageOptions.OnColdFlush != nil {
		opts = opts.SetOnColdFlush(runOpts.StorageOptions.OnColdFlush)
	}

	// Set bootstrap options - We need to create a topology map provider from the
	// same topology that will be passed to the cluster so that when we make
	// bootstrapping decisions they are in sync with the clustered database
	// which is triggering the actual bootstraps. This way, when the clustered
	// database receives a topology update and decides to kick off a bootstrap,
	// the bootstrap process will receaive a topology map that is at least as
	// recent as the one that triggered the bootstrap, if not newer.
	// See GitHub issue #1013 for more details.
	topoMapProvider := newTopoMapProvider(topo)
	bs, err := cfg.Bootstrap.New(config.NewBootstrapConfigurationValidator(),
		rsOpts, opts, topoMapProvider, origin, m3dbClient)
	if err != nil {
		logger.Fatal("could not create bootstrap process", zap.Error(err))
	}

	opts = opts.SetBootstrapProcessProvider(bs)
	timeout := bootstrapConfigInitTimeout

	bsGauge := instrument.NewStringListEmitter(scope, "bootstrappers")
	if err := bsGauge.Start(cfg.Bootstrap.Bootstrappers); err != nil {
		logger.Error("unable to start emitting bootstrap gauge",
			zap.Strings("bootstrappers", cfg.Bootstrap.Bootstrappers),
			zap.Error(err),
		)
	}
	defer func() {
		if err := bsGauge.Close(); err != nil {
			logger.Error("stop emitting bootstrap gauge failed", zap.Error(err))
		}
	}()

	kvWatchBootstrappers(syncCfg.KVStore, logger, timeout, cfg.Bootstrap.Bootstrappers,
		func(bootstrappers []string) {
			if len(bootstrappers) == 0 {
				logger.Error("updated bootstrapper list is empty")
				return
			}

			cfg.Bootstrap.Bootstrappers = bootstrappers
			updated, err := cfg.Bootstrap.New(config.NewBootstrapConfigurationValidator(),
				rsOpts, opts, topoMapProvider, origin, m3dbClient)
			if err != nil {
				logger.Error("updated bootstrapper list failed", zap.Error(err))
				return
			}

			bs.SetBootstrapperProvider(updated.BootstrapperProvider())

			if err := bsGauge.UpdateStringList(bootstrappers); err != nil {
				logger.Error("unable to update bootstrap gauge with new bootstrappers",
					zap.Strings("bootstrappers", bootstrappers),
					zap.Error(err),
				)
			}
		})

	// Start the cluster services now that the M3DB client is available.
	tchannelthriftClusterClose, err := ttcluster.NewServer(m3dbClient,
		cfg.ClusterListenAddress, contextPool, tchannelOpts).ListenAndServe()
	if err != nil {
		logger.Fatal("could not open tchannelthrift interface",
			zap.String("address", cfg.ClusterListenAddress), zap.Error(err))
	}
	defer tchannelthriftClusterClose()
	logger.Info("cluster tchannelthrift: listening", zap.String("address", cfg.ClusterListenAddress))

	httpjsonClusterClose, err := hjcluster.NewServer(m3dbClient,
		cfg.HTTPClusterListenAddress, contextPool, nil).ListenAndServe()
	if err != nil {
		logger.Fatal("could not open httpjson interface",
			zap.String("address", cfg.HTTPClusterListenAddress), zap.Error(err))
	}
	defer httpjsonClusterClose()
	logger.Info("cluster httpjson: listening", zap.String("address", cfg.HTTPClusterListenAddress))

	// Initialize clustered database.
	clusterTopoWatch, err := topo.Watch()
	if err != nil {
		logger.Fatal("could not create cluster topology watch", zap.Error(err))
	}

	opts = opts.SetSchemaRegistry(schemaRegistry)
	db, err := cluster.NewDatabase(hostID, topo, clusterTopoWatch, opts)
	if err != nil {
		logger.Fatal("could not construct database", zap.Error(err))
	}

	// Now that the database has been created it can be set as the block lease verifier
	// on the block lease manager.
	leaseVerifier := storage.NewLeaseVerifier(db)
	blockLeaseManager.SetLeaseVerifier(leaseVerifier)

	if err := db.Open(); err != nil {
		logger.Fatal("could not open database", zap.Error(err))
	}

	// Now that we've initialized the database we can set it on the service.
	service.SetDatabase(db)

	go func() {
		if runOpts.BootstrapCh != nil {
			// Notify on bootstrap chan if specified.
			defer func() {
				runOpts.BootstrapCh <- struct{}{}
			}()
		}

		// Bootstrap asynchronously so we can handle interrupt.
		if err := db.Bootstrap(); err != nil {
			logger.Fatal("could not bootstrap database", zap.Error(err))
		}
		logger.Info("bootstrapped")

		// Only set the write new series limit after bootstrapping
		kvWatchNewSeriesLimitPerShard(syncCfg.KVStore, logger, topo,
			runtimeOptsMgr, cfg.WriteNewSeriesLimitPerSecond)
		kvWatchEncodersPerBlockLimit(syncCfg.KVStore, logger,
			runtimeOptsMgr, cfg.Limits.MaxEncodersPerBlock)
	}()

	// Wait for process interrupt.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: runOpts.InterruptCh,
	})

	// Attempt graceful server close.
	closedCh := make(chan struct{})
	go func() {
		err := db.Terminate()
		if err != nil {
			logger.Error("close database error", zap.Error(err))
		}
		closedCh <- struct{}{}
	}()

	// Wait then close or hard close.
	closeTimeout := serverGracefulCloseTimeout
	select {
	case <-closedCh:
		logger.Info("server closed")
	case <-time.After(closeTimeout):
		logger.Error("server closed after timeout", zap.Duration("timeout", closeTimeout))
	}
}

func bgValidateProcessLimits(logger *zap.Logger) {
	// If unable to validate process limits on the current configuration,
	// do not run background validator task.
	if canValidate, message := canValidateProcessLimits(); !canValidate {
		logger.Warn("cannot validate process limits: invalid configuration found",
			zap.String("message", message))
		return
	}

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

		logger.Warn("invalid configuration found, refer to linked documentation for more information",
			zap.String("url", xdocs.Path("operational_guide/kernel_configuration")),
			zap.Error(err),
		)

		<-t.C
	}
}

func kvWatchNewSeriesLimitPerShard(
	store kv.Store,
	logger *zap.Logger,
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
			logger.Warn("error resolving cluster new series insert limit", zap.Error(err))
		}
		initClusterLimit = defaultClusterNewSeriesLimit
	}

	err = setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, initClusterLimit)
	if err != nil {
		logger.Warn("unable to set cluster new series insert limit", zap.Error(err))
	}

	watch, err := store.Watch(kvconfig.ClusterNewSeriesInsertLimitKey)
	if err != nil {
		logger.Error("could not watch cluster new series insert limit", zap.Error(err))
		return
	}

	go func() {
		protoValue := &commonpb.Int64Proto{}
		for range watch.C() {
			value := defaultClusterNewSeriesLimit
			if newValue := watch.Get(); newValue != nil {
				if err := newValue.Unmarshal(protoValue); err != nil {
					logger.Warn("unable to parse new cluster new series insert limit", zap.Error(err))
					continue
				}
				value = int(protoValue.Value)
			}

			err = setNewSeriesLimitPerShardOnChange(topo, runtimeOptsMgr, value)
			if err != nil {
				logger.Warn("unable to set cluster new series insert limit", zap.Error(err))
				continue
			}
		}
	}()
}

func kvWatchEncodersPerBlockLimit(
	store kv.Store,
	logger *zap.Logger,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	defaultEncodersPerBlockLimit int,
) {
	var initEncoderLimit int

	value, err := store.Get(kvconfig.EncodersPerBlockLimitKey)
	if err == nil {
		protoValue := &commonpb.Int64Proto{}
		err = value.Unmarshal(protoValue)
		if err == nil {
			initEncoderLimit = int(protoValue.Value)
		}
	}

	if err != nil {
		if err != kv.ErrNotFound {
			logger.Warn("error resolving encoder per block limit", zap.Error(err))
		}
		initEncoderLimit = defaultEncodersPerBlockLimit
	}

	err = setEncodersPerBlockLimitOnChange(runtimeOptsMgr, initEncoderLimit)
	if err != nil {
		logger.Warn("unable to set encoder per block limit", zap.Error(err))
	}

	watch, err := store.Watch(kvconfig.EncodersPerBlockLimitKey)
	if err != nil {
		logger.Error("could not watch encoder per block limit", zap.Error(err))
		return
	}

	go func() {
		protoValue := &commonpb.Int64Proto{}
		for range watch.C() {
			value := defaultEncodersPerBlockLimit
			if newValue := watch.Get(); newValue != nil {
				if err := newValue.Unmarshal(protoValue); err != nil {
					logger.Warn("unable to parse new encoder per block limit", zap.Error(err))
					continue
				}
				value = int(protoValue.Value)
			}

			err = setEncodersPerBlockLimitOnChange(runtimeOptsMgr, value)
			if err != nil {
				logger.Warn("unable to set encoder per block limit", zap.Error(err))
				continue
			}
		}
	}()
}

func kvWatchClientConsistencyLevels(
	store kv.Store,
	logger *zap.Logger,
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
	logger *zap.Logger,
	key string,
	onValue func(value string) error,
	onDelete func() error,
) {
	protoValue := &commonpb.StringProto{}

	// First try to eagerly set the value so it doesn't flap if the
	// watch returns but not immediately for an existing value
	value, err := store.Get(key)
	if err != nil && err != kv.ErrNotFound {
		logger.Error("could not resolve KV", zap.String("key", key), zap.Error(err))
	}
	if err == nil {
		if err := value.Unmarshal(protoValue); err != nil {
			logger.Error("could not unmarshal KV key", zap.String("key", key), zap.Error(err))
		} else if err := onValue(protoValue.Value); err != nil {
			logger.Error("could not process value of KV", zap.String("key", key), zap.Error(err))
		} else {
			logger.Info("set KV key", zap.String("key", key), zap.Any("value", protoValue.Value))
		}
	}

	watch, err := store.Watch(key)
	if err != nil {
		logger.Error("could not watch KV key", zap.String("key", key), zap.Error(err))
		return
	}

	go func() {
		for range watch.C() {
			newValue := watch.Get()
			if newValue == nil {
				if err := onDelete(); err != nil {
					logger.Warn("could not set default for KV key", zap.String("key", key), zap.Error(err))
				}
				continue
			}

			err := newValue.Unmarshal(protoValue)
			if err != nil {
				logger.Warn("could not unmarshal KV key", zap.String("key", key), zap.Error(err))
				continue
			}
			if err := onValue(protoValue.Value); err != nil {
				logger.Warn("could not process change for KV key", zap.String("key", key), zap.Error(err))
				continue
			}
			logger.Info("set KV key", zap.String("key", key), zap.Any("value", protoValue.Value))
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

func setEncodersPerBlockLimitOnChange(
	runtimeOptsMgr m3dbruntime.OptionsManager,
	encoderLimit int,
) error {
	runtimeOpts := runtimeOptsMgr.Get()
	if runtimeOpts.EncodersPerBlockLimit() == encoderLimit {
		// Not changed, no need to set the value and trigger a runtime options update
		return nil
	}

	newRuntimeOpts := runtimeOpts.
		SetEncodersPerBlockLimit(encoderLimit)
	return runtimeOptsMgr.Update(newRuntimeOpts)
}

// this function will block for at most waitTimeout to try to get an initial value
// before we kick off the bootstrap
func kvWatchBootstrappers(
	kv kv.Store,
	logger *zap.Logger,
	waitTimeout time.Duration,
	defaultBootstrappers []string,
	onUpdate func(bootstrappers []string),
) {
	vw, err := kv.Watch(kvconfig.BootstrapperKey)
	if err != nil {
		logger.Fatal("could not watch value for key with KV",
			zap.String("key", kvconfig.BootstrapperKey))
	}

	initializedCh := make(chan struct{})

	var initialized bool
	go func() {
		opts := util.NewOptions().SetLogger(logger)

		for range vw.C() {
			v, err := util.StringArrayFromValue(vw.Get(),
				kvconfig.BootstrapperKey, defaultBootstrappers, opts)
			if err != nil {
				logger.Error("error converting KV update to string array",
					zap.String("key", kvconfig.BootstrapperKey),
					zap.Error(err),
				)
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
	logger *zap.Logger,
	opts storage.Options,
	policy config.PoolingPolicy,
) storage.Options {
	iopts := opts.InstrumentOptions()
	scope := opts.InstrumentOptions().MetricsScope()

	// Set the max bytes pool byte slice alloc size for the thrift pooling.
	thriftBytesAllocSize := policy.ThriftBytesPoolAllocSizeOrDefault()
	logger.Info("set thrift bytes pool alloc size",
		zap.Int("size", thriftBytesAllocSize))
	apachethrift.SetMaxBytesPoolAlloc(thriftBytesAllocSize)

	bytesPoolOpts := pool.NewObjectPoolOptions().
		SetInstrumentOptions(iopts.SetMetricsScope(scope.SubScope("bytes-pool")))
	checkedBytesPoolOpts := bytesPoolOpts.
		SetInstrumentOptions(iopts.SetMetricsScope(scope.SubScope("checked-bytes-pool")))

	buckets := make([]pool.Bucket, len(policy.BytesPool.Buckets))
	for i, bucket := range policy.BytesPool.Buckets {
		var b pool.Bucket
		b.Capacity = bucket.CapacityOrDefault()
		b.Count = bucket.SizeOrDefault()
		b.Options = bytesPoolOpts.
			SetRefillLowWatermark(bucket.RefillLowWaterMarkOrDefault()).
			SetRefillHighWatermark(bucket.RefillHighWaterMarkOrDefault())
		buckets[i] = b

		logger.Info("bytes pool configured",
			zap.Int("capacity", bucket.CapacityOrDefault()),
			zap.Int("size", bucket.SizeOrDefault()),
			zap.Float64("refillLowWaterMark", bucket.RefillLowWaterMarkOrDefault()),
			zap.Float64("refillHighWaterMark", bucket.RefillHighWaterMarkOrDefault()))
	}

	var bytesPool pool.CheckedBytesPool
	switch policy.TypeOrDefault() {
	case config.SimplePooling:
		bytesPool = pool.NewCheckedBytesPool(
			buckets,
			checkedBytesPoolOpts,
			func(s []pool.Bucket) pool.BytesPool {
				return pool.NewBytesPool(s, bytesPoolOpts)
			})
	default:
		logger.Fatal("unrecognized pooling type", zap.Any("type", policy.Type))
	}

	{
		// Avoid polluting the rest of the function with `l` var
		l := logger
		if t := policy.Type; t != nil {
			l = l.With(zap.String("policy", string(*t)))
		}

		l.Info("bytes pool init start")
		bytesPool.Init()
		l.Info("bytes pool init end")
	}

	segmentReaderPool := xio.NewSegmentReaderPool(
		poolOptions(
			policy.SegmentReaderPool,
			scope.SubScope("segment-reader-pool")))
	segmentReaderPool.Init()

	encoderPool := encoding.NewEncoderPool(
		poolOptions(
			policy.EncoderPool,
			scope.SubScope("encoder-pool")))

	closersPoolOpts := poolOptions(
		policy.ClosersPool,
		scope.SubScope("closers-pool"))

	contextPoolOpts := poolOptions(
		policy.ContextPool,
		scope.SubScope("context-pool"))

	contextPool := xcontext.NewPool(xcontext.NewOptions().
		SetContextPoolOptions(contextPoolOpts).
		SetFinalizerPoolOptions(closersPoolOpts))

	iteratorPool := encoding.NewReaderIteratorPool(
		poolOptions(
			policy.IteratorPool,
			scope.SubScope("iterator-pool")))

	multiIteratorPool := encoding.NewMultiReaderIteratorPool(
		poolOptions(
			policy.IteratorPool,
			scope.SubScope("multi-iterator-pool")))

	var writeBatchPoolInitialBatchSize *int
	if policy.WriteBatchPool.InitialBatchSize != nil {
		// Use config value if available.
		writeBatchPoolInitialBatchSize = policy.WriteBatchPool.InitialBatchSize
	} else {
		// Otherwise use the default batch size that the client will use.
		clientDefaultSize := client.DefaultWriteBatchSize
		writeBatchPoolInitialBatchSize = &clientDefaultSize
	}

	var writeBatchPoolMaxBatchSize *int
	if policy.WriteBatchPool.MaxBatchSize != nil {
		writeBatchPoolMaxBatchSize = policy.WriteBatchPool.MaxBatchSize
	}

	var writeBatchPoolSize int
	if policy.WriteBatchPool.Size != nil {
		writeBatchPoolSize = *policy.WriteBatchPool.Size
	} else {
		// If no value set, calculate a reasonable value based on the commit log
		// queue size. We base it off the commitlog queue size because we will
		// want to be able to buffer at least one full commitlog queues worth of
		// writes without allocating because these objects are very expensive to
		// allocate.
		commitlogQueueSize := opts.CommitLogOptions().BacklogQueueSize()
		expectedBatchSize := *writeBatchPoolInitialBatchSize
		writeBatchPoolSize = commitlogQueueSize / expectedBatchSize
	}

	writeBatchPoolOpts := pool.NewObjectPoolOptions()
	writeBatchPoolOpts = writeBatchPoolOpts.
		SetSize(writeBatchPoolSize).
		// Set watermarks to zero because this pool is sized to be as large as we
		// ever need it to be, so background allocations are usually wasteful.
		SetRefillLowWatermark(0.0).
		SetRefillHighWatermark(0.0).
		SetInstrumentOptions(
			writeBatchPoolOpts.
				InstrumentOptions().
				SetMetricsScope(scope.SubScope("write-batch-pool")))

	writeBatchPool := writes.NewWriteBatchPool(
		writeBatchPoolOpts,
		writeBatchPoolInitialBatchSize,
		writeBatchPoolMaxBatchSize)

	tagPoolPolicy := policy.TagsPool
	identifierPool := ident.NewPool(bytesPool, ident.PoolOptions{
		IDPoolOptions: poolOptions(
			policy.IdentifierPool, scope.SubScope("identifier-pool")),
		TagsPoolOptions: maxCapacityPoolOptions(tagPoolPolicy, scope.SubScope("tags-pool")),
		TagsCapacity:    tagPoolPolicy.CapacityOrDefault(),
		TagsMaxCapacity: tagPoolPolicy.MaxCapacityOrDefault(),
		TagsIteratorPoolOptions: poolOptions(
			policy.TagsIteratorPool,
			scope.SubScope("tags-iterator-pool")),
	})

	fetchBlockMetadataResultsPoolPolicy := policy.FetchBlockMetadataResultsPool
	fetchBlockMetadataResultsPool := block.NewFetchBlockMetadataResultsPool(
		capacityPoolOptions(
			fetchBlockMetadataResultsPoolPolicy,
			scope.SubScope("fetch-block-metadata-results-pool")),
		fetchBlockMetadataResultsPoolPolicy.CapacityOrDefault())

	fetchBlocksMetadataResultsPoolPolicy := policy.FetchBlocksMetadataResultsPool
	fetchBlocksMetadataResultsPool := block.NewFetchBlocksMetadataResultsPool(
		capacityPoolOptions(
			fetchBlocksMetadataResultsPoolPolicy,
			scope.SubScope("fetch-blocks-metadata-results-pool")),
		fetchBlocksMetadataResultsPoolPolicy.CapacityOrDefault())

	bytesWrapperPoolOpts := poolOptions(
		policy.CheckedBytesWrapperPool,
		scope.SubScope("checked-bytes-wrapper-pool"))
	bytesWrapperPool := xpool.NewCheckedBytesWrapperPool(
		bytesWrapperPoolOpts)
	bytesWrapperPool.Init()

	encodingOpts := encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetBytesPool(bytesPool).
		SetSegmentReaderPool(segmentReaderPool).
		SetCheckedBytesWrapperPool(bytesWrapperPool)

	encoderPool.Init(func() encoding.Encoder {
		if cfg.Proto != nil && cfg.Proto.Enabled {
			enc := proto.NewEncoder(time.Time{}, encodingOpts)
			return enc
		}

		return m3tsz.NewEncoder(time.Time{}, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	iteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		if cfg.Proto != nil && cfg.Proto.Enabled {
			return proto.NewIterator(r, descr, encodingOpts)
		}
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	multiIteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		iter := iteratorPool.Get()
		iter.Reset(r, descr)
		return iter
	})

	writeBatchPool.Init()

	bucketPool := series.NewBufferBucketPool(
		poolOptions(policy.BufferBucketPool, scope.SubScope("buffer-bucket-pool")))
	bucketVersionsPool := series.NewBufferBucketVersionsPool(
		poolOptions(policy.BufferBucketVersionsPool, scope.SubScope("buffer-bucket-versions-pool")))

	retrieveRequestPool := fs.NewRetrieveRequestPool(segmentReaderPool,
		poolOptions(policy.RetrieveRequestPool, scope.SubScope("retrieve-request-pool")))
	retrieveRequestPool.Init()

	opts = opts.
		SetBytesPool(bytesPool).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
		SetIdentifierPool(identifierPool).
		SetFetchBlockMetadataResultsPool(fetchBlockMetadataResultsPool).
		SetFetchBlocksMetadataResultsPool(fetchBlocksMetadataResultsPool).
		SetWriteBatchPool(writeBatchPool).
		SetBufferBucketPool(bucketPool).
		SetBufferBucketVersionsPool(bucketVersionsPool).
		SetRetrieveRequestPool(retrieveRequestPool).
		SetCheckedBytesWrapperPool(bytesWrapperPool)

	blockOpts := opts.DatabaseBlockOptions().
		SetDatabaseBlockAllocSize(policy.BlockAllocSizeOrDefault()).
		SetContextPool(contextPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
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
	blockPool := block.NewDatabaseBlockPool(
		poolOptions(
			policy.BlockPool,
			scope.SubScope("block-pool")))
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts, namespace.Context{})
	})
	blockOpts = blockOpts.SetDatabaseBlockPool(blockPool)
	opts = opts.SetDatabaseBlockOptions(blockOpts)

	// NB(prateek): retention opts are overridden per namespace during series creation
	retentionOpts := retention.NewOptions()
	seriesOpts := storage.NewSeriesOptionsFromOptions(opts, retentionOpts).
		SetFetchBlockMetadataResultsPool(opts.FetchBlockMetadataResultsPool())
	seriesPool := series.NewDatabaseSeriesPool(
		poolOptions(
			policy.SeriesPool,
			scope.SubScope("series-pool")))

	opts = opts.
		SetSeriesOptions(seriesOpts).
		SetDatabaseSeriesPool(seriesPool)
	opts = opts.SetCommitLogOptions(opts.CommitLogOptions().
		SetBytesPool(bytesPool).
		SetIdentifierPool(identifierPool))

	postingsListOpts := poolOptions(policy.PostingsListPool, scope.SubScope("postingslist-pool"))
	postingsList := postings.NewPool(postingsListOpts, roaring.NewPostingsList)

	queryResultsPool := index.NewQueryResultsPool(
		poolOptions(policy.IndexResultsPool, scope.SubScope("index-query-results-pool")))
	aggregateQueryResultsPool := index.NewAggregateResultsPool(
		poolOptions(policy.IndexResultsPool, scope.SubScope("index-aggregate-results-pool")))
	aggregateQueryValuesPool := index.NewAggregateValuesPool(
		poolOptions(policy.IndexResultsPool, scope.SubScope("index-aggregate-values-pool")))

	// Set value transformation options.
	opts = opts.SetTruncateType(cfg.Transforms.TruncateBy)
	forcedValue := cfg.Transforms.ForcedValue
	if forcedValue != nil {
		opts = opts.SetWriteTransformOptions(series.WriteTransformOptions{
			ForceValueEnabled: true,
			ForceValue:        *forcedValue,
		})
	}

	// Set index options.
	indexOpts := opts.IndexOptions().
		SetInstrumentOptions(iopts).
		SetMemSegmentOptions(
			opts.IndexOptions().MemSegmentOptions().
				SetPostingsListPool(postingsList).
				SetInstrumentOptions(iopts)).
		SetFSTSegmentOptions(
			opts.IndexOptions().FSTSegmentOptions().
				SetPostingsListPool(postingsList).
				SetInstrumentOptions(iopts).
				SetContextPool(opts.ContextPool())).
		SetSegmentBuilderOptions(
			opts.IndexOptions().SegmentBuilderOptions().
				SetPostingsListPool(postingsList)).
		SetIdentifierPool(identifierPool).
		SetCheckedBytesPool(bytesPool).
		SetQueryResultsPool(queryResultsPool).
		SetAggregateResultsPool(aggregateQueryResultsPool).
		SetAggregateValuesPool(aggregateQueryValuesPool).
		SetForwardIndexProbability(cfg.Index.ForwardIndexProbability).
		SetForwardIndexThreshold(cfg.Index.ForwardIndexThreshold)

	queryResultsPool.Init(func() index.QueryResults {
		// NB(r): Need to initialize after setting the index opts so
		// it sees the same reference of the options as is set for the DB.
		return index.NewQueryResults(nil, index.QueryResultsOptions{}, indexOpts)
	})
	aggregateQueryResultsPool.Init(func() index.AggregateResults {
		// NB(r): Need to initialize after setting the index opts so
		// it sees the same reference of the options as is set for the DB.
		return index.NewAggregateResults(nil, index.AggregateResultsOptions{}, indexOpts)
	})
	aggregateQueryValuesPool.Init(func() index.AggregateValues {
		// NB(r): Need to initialize after setting the index opts so
		// it sees the same reference of the options as is set for the DB.
		return index.NewAggregateValues(indexOpts)
	})

	return opts.SetIndexOptions(indexOpts)
}

func newAdminClient(
	config client.Configuration,
	iopts instrument.Options,
	tchannelOpts *tchannel.ChannelOptions,
	topologyInitializer topology.Initializer,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	origin topology.Host,
	protoEnabled bool,
	schemaRegistry namespace.SchemaRegistry,
	kvStore kv.Store,
	logger *zap.Logger,
	custom []client.CustomAdminOption,
) (client.AdminClient, error) {
	if config.EnvironmentConfig != nil {
		// If the user has provided an override for the dynamic client configuration
		// then we need to honor it by not passing our own topology initializer.
		topologyInitializer = nil
	}

	// NB: append custom options coming from run options to existing options.
	options := []client.CustomAdminOption{
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetChannelOptions(tchannelOpts).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetRuntimeOptionsManager(runtimeOptsMgr).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetContextPool(opts.ContextPool()).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetOrigin(origin).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			if protoEnabled {
				return opts.SetEncodingProto(encoding.NewOptions()).(client.AdminOptions)
			}
			return opts
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetSchemaRegistry(schemaRegistry).(client.AdminOptions)
		},
	}

	options = append(options, custom...)
	m3dbClient, err := config.NewAdminClient(
		client.ConfigurationParameters{
			InstrumentOptions: iopts.
				SetMetricsScope(iopts.MetricsScope().SubScope("m3dbclient")),
			TopologyInitializer: topologyInitializer,
		},
		options...,
	)
	if err != nil {
		return nil, err
	}

	// Kick off runtime options manager KV watches.
	clientAdminOpts := m3dbClient.Options().(client.AdminOptions)
	kvWatchClientConsistencyLevels(kvStore, logger,
		clientAdminOpts, runtimeOptsMgr)
	return m3dbClient, nil
}

func poolOptions(
	policy config.PoolPolicy,
	scope tally.Scope,
) pool.ObjectPoolOptions {
	var (
		opts                = pool.NewObjectPoolOptions()
		size                = policy.SizeOrDefault()
		refillLowWaterMark  = policy.RefillLowWaterMarkOrDefault()
		refillHighWaterMark = policy.RefillHighWaterMarkOrDefault()
	)

	if size > 0 {
		opts = opts.SetSize(size)
		if refillLowWaterMark > 0 &&
			refillHighWaterMark > 0 &&
			refillHighWaterMark > refillLowWaterMark {
			opts = opts.
				SetRefillLowWatermark(refillLowWaterMark).
				SetRefillHighWatermark(refillHighWaterMark)
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
	var (
		opts                = pool.NewObjectPoolOptions()
		size                = policy.SizeOrDefault()
		refillLowWaterMark  = policy.RefillLowWaterMarkOrDefault()
		refillHighWaterMark = policy.RefillHighWaterMarkOrDefault()
	)

	if size > 0 {
		opts = opts.SetSize(size)
		if refillLowWaterMark > 0 &&
			refillHighWaterMark > 0 &&
			refillHighWaterMark > refillLowWaterMark {
			opts = opts.SetRefillLowWatermark(refillLowWaterMark)
			opts = opts.SetRefillHighWatermark(refillHighWaterMark)
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
	var (
		opts                = pool.NewObjectPoolOptions()
		size                = policy.SizeOrDefault()
		refillLowWaterMark  = policy.RefillLowWaterMarkOrDefault()
		refillHighWaterMark = policy.RefillHighWaterMarkOrDefault()
	)

	if size > 0 {
		opts = opts.SetSize(size)
		if refillLowWaterMark > 0 &&
			refillHighWaterMark > 0 &&
			refillHighWaterMark > refillLowWaterMark {
			opts = opts.SetRefillLowWatermark(refillLowWaterMark)
			opts = opts.SetRefillHighWatermark(refillHighWaterMark)
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
	defer mmap.Munmap(withHugeTLB)

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
	defer mmap.Munmap(withoutHugeTLB)
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

// Ensure mmap reporter implements mmap.Reporter
var _ mmap.Reporter = (*mmapReporter)(nil)

type mmapReporter struct {
	sync.Mutex
	scope   tally.Scope
	entries map[string]*mmapReporterEntry
}

type mmapReporterEntry struct {
	value int64
	gauge tally.Gauge
}

func newMmapReporter(scope tally.Scope) *mmapReporter {
	return &mmapReporter{
		scope:   scope,
		entries: make(map[string]*mmapReporterEntry),
	}
}

func (r *mmapReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Lock()
			for _, r := range r.entries {
				r.gauge.Update(float64(r.value))
			}
			r.Unlock()
		}
	}
}

func (r *mmapReporter) entryKeyAndTags(ctx mmap.Context) (string, map[string]string) {
	numTags := 1
	if ctx.Metadata != nil {
		numTags += len(ctx.Metadata)
	}

	tags := make(map[string]string, numTags)
	tags[mmapReporterTagName] = ctx.Name
	if ctx.Metadata != nil {
		for k, v := range ctx.Metadata {
			tags[k] = v
		}
	}

	entryKey := tally.KeyForStringMap(tags)
	return entryKey, tags
}

func (r *mmapReporter) ReportMap(ctx mmap.Context) error {
	if ctx.Name == "" {
		return fmt.Errorf("report mmap map missing context name: %+v", ctx)
	}

	entryKey, entryTags := r.entryKeyAndTags(ctx)

	r.Lock()
	defer r.Unlock()

	entry, ok := r.entries[entryKey]
	if !ok {
		entry = &mmapReporterEntry{
			gauge: r.scope.Tagged(entryTags).Gauge(mmapReporterMetricName),
		}
		r.entries[entryKey] = entry
	}

	entry.value += ctx.Size

	return nil
}

func (r *mmapReporter) ReportUnmap(ctx mmap.Context) error {
	if ctx.Name == "" {
		return fmt.Errorf("report mmap unmap missing context name: %+v", ctx)
	}

	entryKey, _ := r.entryKeyAndTags(ctx)

	r.Lock()
	defer r.Unlock()

	entry, ok := r.entries[entryKey]
	if !ok {
		return fmt.Errorf("report mmap unmap missing entry for context: %+v", ctx)
	}

	entry.value -= ctx.Size

	if entry.value == 0 {
		// No more similar mmaps active for this context name, garbage collect
		delete(r.entries, entryKey)
	}

	return nil
}
