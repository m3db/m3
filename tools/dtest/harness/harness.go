package harness

import (
	// pprof import
	_ "net/http/pprof"
	"path"
	"strconv"

	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	xclock "github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/tools/dtest/config"
	"github.com/m3db/m3db/tools/dtest/util"
	"github.com/m3db/m3db/tools/dtest/util/seed"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/m3em/convert"
	m3emnode "github.com/m3db/m3db/x/m3em/node"
	"github.com/m3db/m3em/build"
	"github.com/m3db/m3em/cluster"
	hb "github.com/m3db/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3em/node"
	m3xclock "github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	xtcp "github.com/m3db/m3x/tcp"
)

const (
	defaultBootstrapStatusReportingInterval = time.Minute
	agentM3DBDataDir                        = "m3db-data/data"
)

type closeFn func() error

// DTestHarness makes it easier to maintain the lifecycle of
// resources used for DTests.
type DTestHarness struct {
	io.Closer

	sync.Mutex
	cluster cluster.Cluster

	closing          int32
	closers          []closeFn
	cliOpts          *config.Args
	conf             *config.Configuration
	harnessDir       string
	iopts            instrument.Options
	logger           xlog.Logger
	placementService services.PlacementService
	nodeOpts         node.Options
	clusterOpts      cluster.Options
	nodes            []m3emnode.Node
}

// New constructs a new DTestHarness
func New(cliOpts *config.Args, logger xlog.Logger) *DTestHarness {
	dt := &DTestHarness{
		cliOpts: cliOpts,
		logger:  logger,
		iopts:   instrument.NewOptions().SetLogger(logger),
	}

	// create temporary directory for use on local host
	dir, err := ioutil.TempDir("", "dtest")
	if err != nil {
		logger.Fatalf("unable to create temp dir: %v", err.Error())
	}
	dt.harnessDir = dir
	dt.addCloser(func() error {
		return os.RemoveAll(dir)
	})

	// parse configuration
	conf, err := config.New(cliOpts.M3EMConfigPath)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}
	dt.conf = conf
	dt.startPProfServer()

	// make placement service
	pSvc, err := placementService(dt.m3dbServiceID(),
		conf.KV.NewOptions(), defaultPlacementOptions(dt.iopts))
	if err != nil {
		logger.Fatalf("unable to create placement service %v", err)
	}
	dt.placementService = pSvc

	// set default node options
	no := conf.M3EM.Node.Options(dt.iopts)
	dt.nodeOpts = no.SetHeartbeatOptions(
		no.HeartbeatOptions().
			SetEnabled(true).
			SetHeartbeatRouter(dt.newHeartbeatRouter()))

	// parse node configurations
	nodes, err := dt.conf.Nodes(dt.nodeOpts, cliOpts.NumNodes)
	if err != nil {
		logger.Fatalf("unable to create m3db nodes: %v", err)
	}
	dt.nodes = nodes

	// default cluster options
	co := conf.M3EM.Cluster.Options(dt.iopts)
	dt.clusterOpts = co.
		SetPlacementService(pSvc).
		SetServiceBuild(newBuild(logger, cliOpts.M3DBBuildPath)).
		SetServiceConfig(newConfig(logger, cliOpts.M3DBConfigPath)).
		SetSessionToken(cliOpts.SessionToken).
		SetSessionOverride(cliOpts.SessionOverride).
		SetNodeListener(util.NewPullLogsAndPanicListener(logger, dt.harnessDir))

	if cliOpts.InitialReset {
		dt.initialReset()
	}

	return dt
}

func (dt *DTestHarness) initialReset() {
	svcNodes, err := convert.AsServiceNodes(dt.nodes)
	if err != nil {
		dt.logger.Fatalf("unable to cast nodes: %v", err)
	}

	var (
		concurrency = dt.clusterOpts.NodeConcurrency()
		timeout     = dt.clusterOpts.NodeOperationTimeout()
		teardownFn  = func(n node.ServiceNode) error { return n.Teardown() }
		exec        = node.NewConcurrentExecutor(svcNodes, concurrency, timeout, teardownFn)
	)
	if err := exec.Run(); err != nil {
		dt.logger.Fatalf("unable to reset nodes: %v", err)
	}
}

func (dt *DTestHarness) startPProfServer() {
	serverAddress := fmt.Sprintf("0.0.0.0:%d", dt.conf.DTest.DebugPort)
	go func() {
		if err := http.ListenAndServe(serverAddress, nil); err != nil {
			dt.logger.Fatalf("unable to serve debug server: %v", err)
		}
	}()
	dt.logger.Infof("serving pprof endpoints at: %v", serverAddress)
}

// Close releases any resources held by the harness
func (dt *DTestHarness) Close() error {
	var multiErr xerrors.MultiError
	atomic.StoreInt32(&dt.closing, 1)
	for i := len(dt.closers) - 1; i >= 0; i-- {
		closer := dt.closers[i]
		multiErr = multiErr.Add(closer())
	}
	return multiErr.FinalError()
}

// Nodes returns the M3DBNode(s)
func (dt *DTestHarness) Nodes() []m3emnode.Node {
	return dt.nodes
}

// Configuration returns the parsed configuration struct
func (dt *DTestHarness) Configuration() *config.Configuration {
	return dt.conf
}

// Cluster constructs a cluster based on the options set in the harness
func (dt *DTestHarness) Cluster() cluster.Cluster {
	dt.Lock()
	defer dt.Unlock()
	if cluster := dt.cluster; cluster != nil {
		return cluster
	}

	testCluster, err := cluster.New(dt.m3dbNodesAsServiceNodes(), dt.clusterOpts)
	if err != nil {
		dt.logger.Fatalf("unable to create cluster: %v", err)
	}
	dt.addCloser(testCluster.Teardown)
	dt.cluster = testCluster
	return testCluster
}

// ClusterOptions returns the cluster options
func (dt *DTestHarness) ClusterOptions() cluster.Options {
	return dt.clusterOpts
}

// SetClusterOptions sets the cluster options
func (dt *DTestHarness) SetClusterOptions(co cluster.Options) {
	dt.clusterOpts = co
}

// BootstrapTimeout returns the bootstrap timeout configued
func (dt *DTestHarness) BootstrapTimeout() time.Duration {
	return dt.Configuration().DTest.BootstrapTimeout
}

func (dt *DTestHarness) m3dbNodesAsServiceNodes() []node.ServiceNode {
	numNodes := len(dt.nodes)
	nodes := make([]node.ServiceNode, 0, numNodes)
	for _, n := range dt.nodes {
		nodes = append(nodes, n.(node.ServiceNode))
	}
	return nodes
}

// Seed seeds the cluster nodes within the placement with data
func (dt *DTestHarness) Seed(nodes []node.ServiceNode) error {
	c := dt.Cluster()
	if c.Status() == cluster.ClusterStatusUninitialized {
		return fmt.Errorf("cluster must be Setup() prior to seeding it with data")
	}

	seedConfigs := dt.conf.DTest.Seeds
	if len(seedConfigs) == 0 {
		dt.logger.Infof("No seed configurations provided, skipping.")
		return nil
	}

	for _, conf := range seedConfigs {
		if err := dt.seedWithConfig(nodes, conf); err != nil {
			return err
		}
	}

	return nil
}

func delayedNowFn(delay time.Duration) xclock.NowFn {
	if delay == 0 {
		return time.Now
	}

	return func() time.Time {
		return time.Now().Add(-delay)
	}
}

func (dt *DTestHarness) seedWithConfig(nodes []node.ServiceNode, seedConf config.SeedConfig) error {
	dt.logger.Infof("Seeding data with configuration: %+v", seedConf)

	seedDir := path.Join(dt.harnessDir, "seed", seedConf.Namespace)
	if err := os.MkdirAll(seedDir, os.FileMode(0755)|os.ModeDir); err != nil {
		return fmt.Errorf("unable to create seed dir: %v", err)
	}

	iopts := instrument.NewOptions().SetLogger(dt.logger)
	generateOpts := generate.NewOptions().
		SetRetentionPeriod(seedConf.Retention).
		SetBlockSize(seedConf.BlockSize).
		SetFilePathPrefix(seedDir).
		SetClockOptions(xclock.NewOptions().SetNowFn(delayedNowFn(seedConf.Delay)))

	seedDataOpts := seed.NewOptions().
		SetInstrumentOptions(iopts).
		SetGenerateOptions(generateOpts)

	generator := seed.NewGenerator(seedDataOpts)
	outputNamespace := ts.StringID(seedConf.Namespace)
	dt.logger.Infof("generating data to bootstrap with")

	if err := generator.Generate(outputNamespace, seedConf.LocalShardNum); err != nil {
		return fmt.Errorf("unable to generate data: %v", err)
	}
	dt.logger.Infof("generated data")

	generatedDataPath := path.Join(generateOpts.FilePathPrefix(), "data")
	fakeShardDir := newShardDir(generatedDataPath, outputNamespace, seedConf.LocalShardNum)
	localFiles, err := ioutil.ReadDir(fakeShardDir)
	if err != nil {
		return fmt.Errorf("unable to list local shard directory, err: %v", err)
	}

	// transfer the generated data to the remote hosts
	var (
		co           = dt.ClusterOptions()
		placement    = dt.Cluster().Placement()
		concurrency  = co.NodeConcurrency()
		timeout      = co.NodeOperationTimeout()
		transferFunc = func(n node.ServiceNode) error {
			for _, file := range localFiles {
				base := path.Base(file.Name())
				srcPath := path.Join(fakeShardDir, base)
				paths := generatePaths(placement, n, outputNamespace, base)
				if err := n.TransferLocalFile(srcPath, paths, true); err != nil {
					return err
				}
			}
			return nil
		}
	)

	dt.logger.Infof("transferring data to nodes")
	transferDataExecutor := node.NewConcurrentExecutor(nodes, concurrency, timeout, transferFunc)
	if err := transferDataExecutor.Run(); err != nil {
		return fmt.Errorf("unable to transfer generated data, err: %v", err)
	}

	return nil
}

func newShardDir(prefix string, ns ts.ID, shard uint32) string {
	return path.Join(prefix, ns.String(), strconv.FormatUint(uint64(shard), 10))
}

func generatePaths(placement services.ServicePlacement, n node.ServiceNode, ns ts.ID, file string) []string {
	paths := []string{}
	pi, ok := placement.Instance(n.ID())
	if !ok {
		return paths
	}
	shards := pi.Shards().AllIDs()
	for _, s := range shards {
		paths = append(paths, path.Join(newShardDir(agentM3DBDataDir, ns, s), file))
	}
	return paths
}

// WaitUntilAllBootstrapped waits until all the provided nodes are bootstrapped, or
// the configured bootstrap timeout period; whichever is sooner. It returns an error
// indicating if all the nodes finished bootstrapping.
func (dt *DTestHarness) WaitUntilAllBootstrapped(nodes []m3emnode.Node) error {
	watcher := util.NewNodesWatcher(nodes, dt.logger, defaultBootstrapStatusReportingInterval)
	if allBootstrapped := watcher.WaitUntilAll(m3emnode.Node.Bootstrapped, dt.BootstrapTimeout()); !allBootstrapped {
		return fmt.Errorf("unable to bootstrap all nodes, err = %v", watcher.PendingAsError())
	}
	return nil
}

// WaitUntilAllShardsAvailable waits until the placement service has all shards marked
// available, or the configured bootstrap timeout period; whichever is sooner. It returns
// an error indicating if all the nodes finished bootstrapping.
func (dt *DTestHarness) WaitUntilAllShardsAvailable() error {
	allAvailable := m3xclock.WaitUntil(dt.AllShardsAvailable, dt.BootstrapTimeout())
	if !allAvailable {
		return fmt.Errorf("all shards not available")
	}
	return nil
}

// AllShardsAvailable returns if the placement service has all shards marked available
func (dt *DTestHarness) AllShardsAvailable() bool {
	p, _, err := dt.placementService.Placement()
	if err != nil {
		return false
	}

	// for all instances
	// for each shard
	// if any shard is not available, return false
	for _, inst := range p.Instances() {
		for _, s := range inst.Shards().All() {
			if s.State() != shard.Available {
				return false
			}
		}
	}

	// all shards are available
	return true
}

// AnyInstanceShardHasState returns a flag if the placement service has any instance
// with the specified shard state
func (dt *DTestHarness) AnyInstanceShardHasState(id string, state shard.State) bool {
	p, _, err := dt.placementService.Placement()
	if err != nil {
		return false
	}

	inst, ok := p.Instance(id)
	if !ok {
		return false
	}

	for _, s := range inst.Shards().All() {
		if s.State() == state {
			return true
		}
	}

	return false
}

func (dt *DTestHarness) newHeartbeatRouter() node.HeartbeatRouter {
	hbPort := dt.conf.M3EM.HeartbeatPort
	listenAddress := fmt.Sprintf("0.0.0.0:%d", hbPort)
	listener, err := xtcp.NewTCPListener(listenAddress, 3*time.Minute)
	if err != nil {
		dt.logger.Fatalf("could not create TCP Listener: %v", err)
	}
	// listener is closed when hbServer.Serve returns

	hostname, err := os.Hostname()
	if err != nil {
		dt.logger.Fatalf("could not retrieve hostname: %v", err)
	}

	externalAddress := fmt.Sprintf("%s:%d", hostname, hbPort)
	hbRouter := node.NewHeartbeatRouter(externalAddress)
	hbServer := grpc.NewServer(grpc.MaxConcurrentStreams(16384))
	hb.RegisterHeartbeaterServer(hbServer, hbRouter)
	go func() {
		err := hbServer.Serve(listener)
		if err != nil {
			if closing := atomic.LoadInt32(&dt.closing); closing == 0 {
				dt.logger.Fatalf("could not create heartbeat server: %v", err)
			}
			// we're closing the server, which will trigger this path. we don't want to error on it
			dt.logger.Infof("stopping heartbeatserver, err: ", err)
		}
	}()
	dt.logger.Infof("serving HeartbeatRouter at %s (i.e. %s)", listenAddress, externalAddress)

	dt.addCloser(func() error {
		hbServer.GracefulStop()
		return nil
	})
	return hbRouter
}

func (dt *DTestHarness) addCloser(fn closeFn) {
	dt.closers = append(dt.closers, fn)
}

func (dt *DTestHarness) m3dbServiceID() services.ServiceID {
	return services.NewServiceID().
		SetName(dt.conf.DTest.M3DBServiceID).
		SetEnvironment(dt.conf.KV.Env).
		SetZone(dt.conf.KV.Zone)
}

func placementService(
	svcID services.ServiceID,
	eopts etcdclient.Options,
	popts services.PlacementOptions,
) (services.PlacementService, error) {
	kvClient, err := etcdclient.NewConfigServiceClient(eopts)
	if err != nil {
		return nil, fmt.Errorf("unable to create kv client: %v", err)
	}

	topoServices, err := kvClient.Services()
	if err != nil {
		return nil, fmt.Errorf("unable to create topology services: %v", err)
	}

	return topoServices.PlacementService(svcID, popts)
}

func defaultPlacementOptions(iopts instrument.Options) services.PlacementOptions {
	return placement.NewOptions().
		SetIsSharded(true).
		SetLooseRackCheck(true).
		SetAllowPartialReplace(true).
		SetInstrumentOptions(iopts)
}

func newBuild(logger xlog.Logger, filename string) build.ServiceBuild {
	bld := build.NewServiceBuild("m3dbnode", filename)
	logger.Infof("marking service build: %+v", bld)
	return bld
}

func newConfig(logger xlog.Logger, filename string) build.ServiceConfiguration {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Fatalf("unable to read: %v, err: %v", filename, err)
	}
	conf := build.NewServiceConfig("m3dbnode.yaml", bytes)
	logger.Infof("read service config from: %v", filename)
	// TODO(prateek): once the struct is OSS-ed, parse M3DB configuration,
	// and ensure the following fields are correctly overriden/line up from dtest|m3em configs
	// - kv (env|zone)
	// - data directory
	// - seed data configuration for block size, retention
	return conf
}
