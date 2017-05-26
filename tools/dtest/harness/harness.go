package harness

import (
	// pprof import
	_ "net/http/pprof"

	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/tools/dtest/config"
	"github.com/m3db/m3db/x/m3em/convert"
	m3dbnode "github.com/m3db/m3db/x/m3em/node"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3em/build"
	"github.com/m3db/m3em/cluster"
	hb "github.com/m3db/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3em/node"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	xtcp "github.com/m3db/m3x/tcp"
	"google.golang.org/grpc"
)

type closeFn func() error

// DTestHarness makes it easier to maintain the lifecycle of
// resources used for DTests.
type DTestHarness struct {
	io.Closer

	closing          int32
	closers          []closeFn
	cliOpts          *config.CLIOpts
	conf             *config.Configuration
	iopts            instrument.Options
	logger           xlog.Logger
	placementService services.PlacementService
	nodeOpts         node.Options
	clusterOpts      cluster.Options
	nodes            []m3dbnode.Node
}

// New constructs a new DTestHarness
func New(cliOpts *config.CLIOpts, logger xlog.Logger) *DTestHarness {
	dt := &DTestHarness{
		cliOpts: cliOpts,
		logger:  logger,
		iopts:   instrument.NewOptions().SetLogger(logger),
	}

	conf, err := config.New(cliOpts.M3EMConfigPath)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}
	dt.conf = conf
	dt.startPProfServer()

	pSvc, err := placementService(dt.m3dbServiceID(),
		conf.KV.NewOptions(), defaultPlacementOptions(dt.iopts))
	if err != nil {
		logger.Fatalf("unable to create placement service %v", err)
	}
	dt.placementService = pSvc

	no := conf.M3EM.Node.Options(dt.iopts)
	dt.nodeOpts = no.SetHeartbeatOptions(
		no.HeartbeatOptions().
			SetEnabled(true).
			SetHeartbeatRouter(dt.newHeartbeatRouter()))

	nodes, err := dt.conf.M3EM.M3DBNodes(dt.nodeOpts, cliOpts.NumNodes)
	if err != nil {
		logger.Fatalf("unable to create m3db nodes: %v", err)
	}
	dt.nodes = nodes

	dt.clusterOpts = cluster.NewOptions(pSvc, dt.iopts).
		SetServiceBuild(newBuild(logger, cliOpts.M3DBBuildPath)).
		SetServiceConfig(newConfig(logger, cliOpts.M3DBConfigPath)).
		SetSessionToken(cliOpts.SessionToken).
		SetSessionOverride(cliOpts.SessionOverride).
		SetNumShards(conf.DTest.NumShards)

	if cliOpts.InitialReset {
		svcNodes, err := convert.AsServiceNodes(nodes)
		if err != nil {
			logger.Fatalf("unable to cast nodes: %v", err)
		}
		exec := node.NewConcurrentExecutor(svcNodes, dt.clusterOpts.NodeConcurrency(), dt.clusterOpts.NodeOperationTimeout(), func(n node.ServiceNode) error { return n.Teardown() })
		if err := exec.Run(); err != nil {
			logger.Fatalf("unable to reset nodes: %v", err)
		}
	}

	return dt
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
func (dt *DTestHarness) Nodes() []m3dbnode.Node {
	return dt.nodes
}

// Configuration returns the parsed configuration struct
func (dt *DTestHarness) Configuration() *config.Configuration {
	return dt.conf
}

// Cluster constructs a cluster based on the options set in the harness
func (dt *DTestHarness) Cluster() cluster.Cluster {
	testCluster, err := cluster.New(dt.m3dbNodesAsServiceNodes(), dt.clusterOpts)
	if err != nil {
		dt.logger.Fatalf("unable to create cluster: %v", err)
	}
	dt.addCloser(testCluster.Teardown)
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
	return time.Duration(dt.Configuration().DTest.BootstrapTimeoutMins) * time.Minute
}

func (dt *DTestHarness) m3dbNodesAsServiceNodes() []node.ServiceNode {
	numNodes := len(dt.nodes)
	nodes := make([]node.ServiceNode, 0, numNodes)
	for _, n := range dt.nodes {
		nodes = append(nodes, n.(node.ServiceNode))
	}
	return nodes
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
		SetName(dt.conf.M3EM.M3DBServiceID).
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
	return conf
}
