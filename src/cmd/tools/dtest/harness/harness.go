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

package harness

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // _ is used for pprof
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/tools/dtest/config"
	"github.com/m3db/m3/src/cmd/tools/dtest/util"
	"github.com/m3db/m3/src/cmd/tools/dtest/util/seed"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/x/m3em/convert"
	m3emnode "github.com/m3db/m3/src/dbnode/x/m3em/node"
	"github.com/m3db/m3/src/m3em/build"
	"github.com/m3db/m3/src/m3em/cluster"
	hb "github.com/m3db/m3/src/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3/src/m3em/node"
	xgrpc "github.com/m3db/m3/src/m3em/x/grpc"
	xclock "github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtcp "github.com/m3db/m3/src/x/tcp"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	buildFilename  = "m3dbnode"
	configFilename = "m3dbnode.yaml"
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
	logger           *zap.Logger
	placementService placement.Service
	nodeOpts         node.Options
	clusterOpts      cluster.Options
	nodes            []m3emnode.Node
}

// New constructs a new DTestHarness
func New(cliOpts *config.Args, rawLogger *zap.Logger) *DTestHarness {
	dt := &DTestHarness{
		cliOpts: cliOpts,
		logger:  rawLogger,
		iopts:   instrument.NewOptions().SetLogger(rawLogger),
	}

	logger := rawLogger.Sugar()
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
	conf, err := config.New(cliOpts.DTestConfigPath)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}
	dt.conf = conf
	dt.startPProfServer()

	zone, err := conf.Zone()
	if err != nil {
		logger.Fatalf("unable to read configuration zone: %v", err)
	}

	// make kv config
	var (
		svcID = dt.serviceID()
		eopts = conf.KV.NewOptions()
		popts = defaultPlacementOptions(zone, dt.iopts)
	)
	kvClient, err := etcdclient.NewConfigServiceClient(eopts)
	if err != nil {
		logger.Fatalf("unable to create kv client: %v", err)
	}

	// set the namespace in kv
	kvStore, err := kvClient.KV()
	if err != nil {
		logger.Fatalf("unable to create kv store: %v", err)
	}

	protoValue, err := defaultNamespaceProtoValue()
	if err != nil {
		logger.Fatalf("unable to create proto value: %v", err)
	}

	_, err = kvStore.Set(kvconfig.NamespacesKey, protoValue)
	if err != nil {
		logger.Fatalf("unable to set initial namespace value: %v", err)
	}

	// register cleanup in the end
	dt.addCloser(func() error {
		_, err := kvStore.Delete(kvconfig.NamespacesKey)
		return err
	})

	// make placement service
	topoServices, err := kvClient.Services(nil)
	if err != nil {
		logger.Fatalf("unable to create topology services: %v", err)
	}
	pSvc, err := topoServices.PlacementService(svcID, popts)
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
		logger.Fatalf("unable to create m3em nodes: %v", err)
	}
	dt.nodes = nodes

	// default cluster options
	co := conf.M3EM.Cluster.Options(dt.iopts)
	dt.clusterOpts = co.
		SetPlacementService(pSvc).
		SetServiceBuild(newBuild(rawLogger, cliOpts.NodeBuildPath)).
		SetServiceConfig(newConfig(rawLogger, cliOpts.NodeConfigPath)).
		SetSessionToken(cliOpts.SessionToken).
		SetSessionOverride(cliOpts.SessionOverride).
		SetNodeListener(util.NewPullLogsAndPanicListener(rawLogger, dt.harnessDir))

	if cliOpts.InitialReset {
		dt.initialReset()
	}

	return dt
}

func (dt *DTestHarness) initialReset() {
	svcNodes, err := convert.AsServiceNodes(dt.nodes)
	if err != nil {
		dt.logger.Fatal("unable to cast nodes", zap.Error(err))
	}

	var (
		concurrency = dt.clusterOpts.NodeConcurrency()
		timeout     = dt.clusterOpts.NodeOperationTimeout()
		teardownFn  = func(n node.ServiceNode) error { return n.Teardown() }
		exec        = node.NewConcurrentExecutor(svcNodes, concurrency, timeout, teardownFn)
	)
	if err := exec.Run(); err != nil {
		dt.logger.Fatal("unable to reset nodes", zap.Error(err))
	}
}

func (dt *DTestHarness) startPProfServer() {
	serverAddress := fmt.Sprintf("0.0.0.0:%d", dt.conf.DTest.DebugPort)
	go func() {
		if err := http.ListenAndServe(serverAddress, nil); err != nil {
			dt.logger.Fatal("unable to serve debug server", zap.Error(err))
		}
	}()
	dt.logger.Info("serving pprof endpoints", zap.String("address", serverAddress))
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

// Nodes returns the m3emnode.Node(s)
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

	svcNodes, err := convert.AsServiceNodes(dt.nodes)
	if err != nil {
		dt.logger.Fatal("unable to cast nodes", zap.Error(err))
	}

	testCluster, err := cluster.New(svcNodes, dt.clusterOpts)
	if err != nil {
		dt.logger.Fatal("unable to create cluster", zap.Error(err))
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

// Seed seeds the cluster nodes within the placement with data
func (dt *DTestHarness) Seed(nodes []node.ServiceNode) error {
	c := dt.Cluster()
	if c.Status() == cluster.ClusterStatusUninitialized {
		return fmt.Errorf("cluster must be Setup() prior to seeding it with data")
	}

	seedConfigs := dt.conf.DTest.Seeds
	if len(seedConfigs) == 0 {
		dt.logger.Info("no seed configurations provided, skipping.")
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
	dt.logger.Info("seeding data with configuration", zap.Any("config", seedConf))

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
	outputNamespace := ident.StringID(seedConf.Namespace)

	if err := generator.Generate(namespace.Context{ID: outputNamespace}, seedConf.LocalShardNum); err != nil {
		return fmt.Errorf("unable to generate data: %v", err)
	}

	generatedDataPath := path.Join(generateOpts.FilePathPrefix(), "data")
	fakeShardDir := newShardDir(generatedDataPath, outputNamespace, seedConf.LocalShardNum)
	localFiles, err := ioutil.ReadDir(fakeShardDir)
	if err != nil {
		return fmt.Errorf("unable to list local shard directory, err: %v", err)
	}

	// transfer the generated data to the remote hosts
	var (
		dataDir      = dt.conf.DTest.DataDir
		co           = dt.ClusterOptions()
		placement    = dt.Cluster().Placement()
		concurrency  = co.NodeConcurrency()
		timeout      = co.NodeOperationTimeout()
		transferFunc = func(n node.ServiceNode) error {
			for _, file := range localFiles {
				base := path.Base(file.Name())
				srcPath := path.Join(fakeShardDir, base)
				paths := generatePaths(placement, n, outputNamespace, base, dataDir)
				if err := n.TransferLocalFile(srcPath, paths, true); err != nil {
					return err
				}
			}
			return nil
		}
	)

	dt.logger.Info("transferring data to nodes")
	transferDataExecutor := node.NewConcurrentExecutor(nodes, concurrency, timeout, transferFunc)
	if err := transferDataExecutor.Run(); err != nil {
		return fmt.Errorf("unable to transfer generated data, err: %v", err)
	}

	return nil
}

func newShardDir(prefix string, ns ident.ID, shard uint32) string {
	return path.Join(prefix, ns.String(), strconv.FormatUint(uint64(shard), 10))
}

func generatePaths(
	placement placement.Placement,
	n node.ServiceNode,
	ns ident.ID,
	file string,
	dataDir string,
) []string {
	paths := []string{}
	pi, ok := placement.Instance(n.ID())
	if !ok {
		return paths
	}
	shards := pi.Shards().AllIDs()
	for _, s := range shards {
		paths = append(paths, path.Join(newShardDir(dataDir, ns, s), file))
	}
	return paths
}

// WaitUntilAllBootstrapped waits until all the provided nodes are bootstrapped, or
// the configured bootstrap timeout period; whichever is sooner. It returns an error
// indicating if all the nodes finished bootstrapping.
func (dt *DTestHarness) WaitUntilAllBootstrapped(nodes []node.ServiceNode) error {
	m3emnodes, err := convert.AsNodes(nodes)
	if err != nil {
		return fmt.Errorf("unable to cast nodes: %v", err)
	}

	watcher := util.NewNodesWatcher(m3emnodes, dt.logger, dt.conf.DTest.BootstrapReportInterval)
	if allBootstrapped := watcher.WaitUntilAll(m3emnode.Node.Bootstrapped, dt.BootstrapTimeout()); !allBootstrapped {
		return fmt.Errorf("unable to bootstrap all nodes, err = %v", watcher.PendingAsError())
	}
	return nil
}

// WaitUntilAllShardsAvailable waits until the placement service has all shards marked
// available, or the configured bootstrap timeout period; whichever is sooner. It returns
// an error indicating if all the nodes finished bootstrapping.
func (dt *DTestHarness) WaitUntilAllShardsAvailable() error {
	allAvailable := xclock.WaitUntil(dt.AllShardsAvailable, dt.BootstrapTimeout())
	if !allAvailable {
		return fmt.Errorf("all shards not available")
	}
	return nil
}

// AllShardsAvailable returns if the placement service has all shards marked available
func (dt *DTestHarness) AllShardsAvailable() bool {
	p, err := dt.placementService.Placement()
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
	p, err := dt.placementService.Placement()
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
		dt.logger.Fatal("could not create TCP Listener", zap.Error(err))
	}
	// listener is closed when hbServer.Serve returns

	hostname, err := os.Hostname()
	if err != nil {
		dt.logger.Fatal("could not retrieve hostname", zap.Error(err))
	}

	externalAddress := fmt.Sprintf("%s:%d", hostname, hbPort)
	hbRouter := node.NewHeartbeatRouter(externalAddress)
	hbServer := xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(hbServer, hbRouter)
	go func() {
		err := hbServer.Serve(listener)
		if err != nil {
			if closing := atomic.LoadInt32(&dt.closing); closing == 0 {
				dt.logger.Fatal("could not create heartbeat server", zap.Error(err))
			}
			// we're closing the server, which will trigger this path. we don't want to error on it
			dt.logger.Info("stopping heartbeatserver, server closed or inaccessible", zap.Error(err))
		}
	}()
	dt.logger.Info("serving HeartbeatRouter",
		zap.String("address", listenAddress), zap.String("external", externalAddress))

	dt.addCloser(func() error {
		hbServer.GracefulStop()
		return nil
	})
	return hbRouter
}

func (dt *DTestHarness) addCloser(fn closeFn) {
	dt.closers = append(dt.closers, fn)
}

func (dt *DTestHarness) serviceID() services.ServiceID {
	return services.NewServiceID().
		SetName(dt.conf.DTest.ServiceID).
		SetEnvironment(dt.conf.KV.Env).
		SetZone(dt.conf.KV.Zone)
}

func defaultPlacementOptions(zone string, iopts instrument.Options) placement.Options {
	return placement.NewOptions().
		SetIsSharded(true).
		SetAllowPartialReplace(true).
		SetInstrumentOptions(iopts).
		SetValidZone(zone)
}

func newBuild(logger *zap.Logger, filename string) build.ServiceBuild {
	bld := build.NewServiceBuild(buildFilename, filename)
	logger.Info("marking service build", zap.Any("build", bld))
	return bld
}

func newConfig(logger *zap.Logger, filename string) build.ServiceConfiguration {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Fatal("unable to read", zap.String("filename", filename), zap.Error(err))
	}
	conf := build.NewServiceConfig(configFilename, bytes)
	logger.Info("read service config", zap.String("filename", filename))
	// TODO(prateek): once the main struct is OSS-ed, parse M3DB configuration,
	// and ensure the following fields are correctly overridden/line up from dtest|m3em configs
	// - kv (env|zone)
	// - data directory
	// - seed data configuration for block size, retention
	return conf
}

func defaultNamespaceProtoValue() (proto.Message, error) {
	md, err := namespace.NewMetadata(
		ident.StringID("metrics"),
		namespace.NewOptions().
			SetBootstrapEnabled(true).
			SetCleanupEnabled(true).
			SetFlushEnabled(true).
			SetRepairEnabled(true).
			SetWritesToCommitLog(true).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(2*time.Hour).
					SetRetentionPeriod(48*time.Hour)))
	if err != nil {
		return nil, err
	}
	nsMap, err := namespace.NewMap([]namespace.Metadata{md})
	if err != nil {
		return nil, err
	}

	registry, err := namespace.ToProto(nsMap)
	if err != nil {
		return nil, err
	}

	return registry, nil
}
