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

package integration

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration/fake"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/cluster"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/require"
	tchannel "github.com/uber/tchannel-go"
)

var (
	id                  = flag.String("id", "", "Node host ID")
	httpClusterAddr     = flag.String("clusterhttpaddr", "127.0.0.1:9000", "Cluster HTTP server address")
	tchannelClusterAddr = flag.String("clustertchanneladdr", "127.0.0.1:9001", "Cluster TChannel server address")
	httpNodeAddr        = flag.String("nodehttpaddr", "127.0.0.1:9002", "Node HTTP server address")
	tchannelNodeAddr    = flag.String("nodetchanneladdr", "127.0.0.1:9003", "Node TChannel server address")
	httpDebugAddr       = flag.String("debughttpaddr", "127.0.0.1:9004", "HTTP debug server address")

	errServerStartTimedOut = errors.New("server took too long to start")
	errServerStopTimedOut  = errors.New("server took too long to stop")
	testNamespaces         = []ident.ID{ident.StringID("testNs1"), ident.StringID("testNs2")}

	created = uint64(0)
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

var _ topology.MapProvider = &testSetup{}

type testSetup struct {
	t    *testing.T
	opts testOptions

	logger xlog.Logger

	db             cluster.Database
	storageOpts    storage.Options
	fsOpts         fs.Options
	hostID         string
	origin         topology.Host
	topoInit       topology.Initializer
	shardSet       sharding.ShardSet
	getNowFn       clock.NowFn
	setNowFn       nowSetterFn
	tchannelClient rpc.TChanNode
	m3dbClient     client.Client
	// We need two distinct clients where one has the origin set to the same ID as the
	// node itself (I.E) the client will behave exactly as if it is the node itself
	// making requests, and another client with the origin set to an ID different than
	// the node itself so that we can make requests from the perspective of a "different"
	// M3DB node for verification purposes in some of the tests.
	m3dbAdminClient             client.AdminClient
	m3dbVerificationAdminClient client.AdminClient
	workerPool                  xsync.WorkerPool

	// things that need to be cleaned up
	channel        *tchannel.Channel
	filePathPrefix string
	namespaces     []namespace.Metadata

	// signals
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestSetup(t *testing.T, opts testOptions, fsOpts fs.Options) (*testSetup, error) {
	if opts == nil {
		opts = newTestOptions(t)
	}

	nsInit := opts.NamespaceInitializer()
	if nsInit == nil {
		nsInit = namespace.NewStaticInitializer(opts.Namespaces())
	}

	var logger xlog.Logger
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		parsedLevel, err := xlog.ParseLevel(level)
		if err != nil {
			return nil, fmt.Errorf("unable to parse log level: %v", err)
		}
		logger = xlog.NewLevelLogger(xlog.SimpleLogger, parsedLevel)
	} else {
		logger = xlog.NewLevelLogger(xlog.SimpleLogger, xlog.LevelInfo)
	}

	storageOpts := storage.NewOptions().
		SetNamespaceInitializer(nsInit).
		SetMinimumSnapshotInterval(opts.MinimumSnapshotInterval())

	// Use specified series cache policy from environment if set.
	seriesCachePolicy := strings.ToLower(os.Getenv("TEST_SERIES_CACHE_POLICY"))
	if seriesCachePolicy != "" {
		value, err := series.ParseCachePolicy(seriesCachePolicy)
		if err != nil {
			return nil, err
		}
		storageOpts = storageOpts.SetSeriesCachePolicy(value)
	}

	fields := []xlog.Field{
		xlog.NewField("cache-policy", storageOpts.SeriesCachePolicy().String()),
	}
	logger = logger.WithFields(fields...)
	iOpts := storageOpts.InstrumentOptions()
	storageOpts = storageOpts.SetInstrumentOptions(iOpts.SetLogger(logger))

	indexMode := index.InsertSync
	if opts.WriteNewSeriesAsync() {
		indexMode = index.InsertAsync
	}
	storageOpts = storageOpts.SetIndexOptions(storageOpts.IndexOptions().SetInsertMode(indexMode))

	runtimeOptsMgr := storageOpts.RuntimeOptionsManager()
	runtimeOpts := runtimeOptsMgr.Get().
		SetTickMinimumInterval(opts.TickMinimumInterval()).
		SetMaxWiredBlocks(opts.MaxWiredBlocks()).
		SetWriteNewSeriesAsync(opts.WriteNewSeriesAsync())
	if err := runtimeOptsMgr.Update(runtimeOpts); err != nil {
		return nil, err
	}

	// Set up shard set
	shardSet, err := newTestShardSet(opts.NumShards())
	if err != nil {
		return nil, err
	}

	id := *id
	if id == "" {
		id = opts.ID()
	}

	tchannelNodeAddr := *tchannelNodeAddr
	if addr := opts.TChannelNodeAddr(); addr != "" {
		tchannelNodeAddr = addr
	}

	topoInit := opts.ClusterDatabaseTopologyInitializer()
	if topoInit == nil {
		topoInit, err = newTopologyInitializerForShardSet(id, tchannelNodeAddr, shardSet)
		if err != nil {
			return nil, err
		}
	}

	adminClient, verificationAdminClient, err := newClients(topoInit, opts, id, tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up tchannel client
	channel, tc, err := tchannelClient(tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up worker pool
	workerPool := xsync.NewWorkerPool(opts.WorkerPoolSize())
	workerPool.Init()

	// BlockSizes are specified per namespace, make best effort at finding
	// a value to align `now` for all of them.
	truncateSize, guess := guessBestTruncateBlockSize(opts.Namespaces())
	if guess {
		logger.Warnf(
			"unable to find a single blockSize from known retention periods, guessing: %v",
			truncateSize.String())
	}

	// Set up getter and setter for now
	var lock sync.RWMutex
	now := time.Now().Truncate(truncateSize)
	getNowFn := func() time.Time {
		lock.RLock()
		t := now
		lock.RUnlock()
		return t
	}
	setNowFn := func(t time.Time) {
		lock.Lock()
		now = t
		lock.Unlock()
	}
	storageOpts = storageOpts.SetClockOptions(
		storageOpts.ClockOptions().SetNowFn(getNowFn))

	// Set up file path prefix
	idx := atomic.AddUint64(&created, 1) - 1
	filePathPrefix := opts.FilePathPrefix()
	if filePathPrefix == "" {
		var err error
		filePathPrefix, err = ioutil.TempDir("", fmt.Sprintf("integration-test-%d", idx))
		if err != nil {
			return nil, err
		}
	}

	if fsOpts == nil {
		fsOpts = fs.NewOptions().
			SetFilePathPrefix(filePathPrefix)
	}

	storageOpts = storageOpts.SetCommitLogOptions(
		storageOpts.CommitLogOptions().
			SetFilesystemOptions(fsOpts).
			SetBlockSize(opts.CommitLogBlockSize()))

	// Set up persistence manager
	pm, err := fs.NewPersistManager(fsOpts)
	if err != nil {
		return nil, err
	}
	storageOpts = storageOpts.SetPersistManager(pm)

	// Set up repair options
	storageOpts = storageOpts.SetRepairOptions(storageOpts.RepairOptions().SetAdminClient(adminClient))

	// Set up block retriever manager
	if mgr := opts.DatabaseBlockRetrieverManager(); mgr != nil {
		storageOpts = storageOpts.SetDatabaseBlockRetrieverManager(mgr)
	} else {
		switch storageOpts.SeriesCachePolicy() {
		case series.CacheAll:
			// Do not need a block retriever for CacheAll policy
		default:
			blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
				func(md namespace.Metadata) (block.DatabaseBlockRetriever, error) {
					retrieverOpts := fs.NewBlockRetrieverOptions()
					retriever := fs.NewBlockRetriever(retrieverOpts, fsOpts)
					if err := retriever.Open(md); err != nil {
						return nil, err
					}
					return retriever, nil
				})
			storageOpts = storageOpts.
				SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
		}
	}

	// Set up wired list if required
	if storageOpts.SeriesCachePolicy() == series.CacheLRU {
		wiredList := block.NewWiredList(block.WiredListOptions{
			RuntimeOptionsManager: runtimeOptsMgr,
			InstrumentOptions:     storageOpts.InstrumentOptions(),
			ClockOptions:          storageOpts.ClockOptions(),
			// Use a small event channel size to stress-test the implementation
			EventsChannelSize: 1,
		})
		blockOpts := storageOpts.DatabaseBlockOptions().SetWiredList(wiredList)
		blockPool := block.NewDatabaseBlockPool(nil)
		// Have to manually set the blockpool because the default one uses a constructor
		// function that doesn't have the updated blockOpts.
		blockPool.Init(func() block.DatabaseBlock {
			return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts)
		})
		blockOpts = blockOpts.SetDatabaseBlockPool(blockPool)
		storageOpts = storageOpts.SetDatabaseBlockOptions(blockOpts)
	}

	// Set debugging options if environment vars set
	if debugFilePrefix := os.Getenv("TEST_DEBUG_FILE_PREFIX"); debugFilePrefix != "" {
		opts = opts.SetVerifySeriesDebugFilePathPrefix(debugFilePrefix)
	}

	return &testSetup{
		t:                           t,
		opts:                        opts,
		logger:                      logger,
		storageOpts:                 storageOpts,
		fsOpts:                      fsOpts,
		hostID:                      id,
		origin:                      newOrigin(id, tchannelNodeAddr),
		topoInit:                    topoInit,
		shardSet:                    shardSet,
		getNowFn:                    getNowFn,
		setNowFn:                    setNowFn,
		tchannelClient:              tc,
		m3dbClient:                  adminClient.(client.Client),
		m3dbAdminClient:             adminClient,
		m3dbVerificationAdminClient: verificationAdminClient,
		workerPool:                  workerPool,
		channel:                     channel,
		filePathPrefix:              filePathPrefix,
		namespaces:                  opts.Namespaces(),
		doneCh:                      make(chan struct{}),
		closedCh:                    make(chan struct{}),
	}, nil
}

// guestBestTruncateBlockSize guesses for the best block size to truncate testSetup's nowFn
func guessBestTruncateBlockSize(mds []namespace.Metadata) (time.Duration, bool) {
	// gcd of a pair of numbers
	gcd := func(a, b int64) int64 {
		for b > 0 {
			a, b = b, a%b
		}
		return a
	}
	lcm := func(a, b int64) int64 {
		return a * b / gcd(a, b)
	}

	// default guess
	if len(mds) == 0 {
		return time.Hour, true
	}

	// get all known blocksizes
	blockSizes := make(map[int64]struct{})
	for _, md := range mds {
		bs := md.Options().RetentionOptions().BlockSize().Nanoseconds() / int64(time.Millisecond)
		blockSizes[bs] = struct{}{}
	}

	first := true
	var l int64
	for i := range blockSizes {
		if first {
			l = i
			first = false
		} else {
			l = lcm(l, i)
		}
	}

	guess := time.Duration(l) * time.Millisecond
	// if there's only a single value, we are not guessing
	if len(blockSizes) == 1 {
		return guess, false
	}

	// otherwise, we are guessing
	return guess, true
}

func (ts *testSetup) namespaceMetadataOrFail(id ident.ID) namespace.Metadata {
	for _, md := range ts.namespaces {
		if md.ID().Equal(id) {
			return md
		}
	}
	require.FailNow(ts.t, "unable to find namespace", id.String())
	return nil
}

func (ts *testSetup) generatorOptions(ropts retention.Options) generate.Options {
	var (
		storageOpts = ts.storageOpts
		fsOpts      = storageOpts.CommitLogOptions().FilesystemOptions()
		opts        = generate.NewOptions()
		co          = opts.ClockOptions().SetNowFn(ts.getNowFn)
	)

	return opts.
		SetClockOptions(co).
		SetRetentionPeriod(ropts.RetentionPeriod()).
		SetBlockSize(ropts.BlockSize()).
		SetFilePathPrefix(fsOpts.FilePathPrefix()).
		SetNewFileMode(fsOpts.NewFileMode()).
		SetNewDirectoryMode(fsOpts.NewDirectoryMode()).
		SetWriterBufferSize(fsOpts.WriterBufferSize()).
		SetEncoderPool(storageOpts.EncoderPool())
}

func (ts *testSetup) serverIsBootstrapped() bool {
	resp, err := ts.health()
	return err == nil && resp.Bootstrapped
}

func (ts *testSetup) serverIsUp() bool {
	_, err := ts.health()
	return err == nil
}

func (ts *testSetup) serverIsDown() bool {
	return !ts.serverIsUp()
}

func (ts *testSetup) waitUntilServerIsBootstrapped() error {
	if waitUntil(ts.serverIsBootstrapped, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsUp() error {
	if waitUntil(ts.serverIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	if waitUntil(ts.serverIsDown, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServerDontWaitBootstrap() error {
	return ts.startServerBase(false)
}

func (ts *testSetup) startServer() error {
	return ts.startServerBase(true)
}

func (ts *testSetup) startServerBase(waitForBootstrap bool) error {
	ts.logger.Infof("starting server")

	var (
		resultCh = make(chan error, 1)
		err      error
	)

	topo, err := ts.topoInit.Init()
	if err != nil {
		return fmt.Errorf("error initializing topology: %v", err)
	}

	topoWatch, err := topo.Watch()
	if err != nil {
		return fmt.Errorf("error watching topology: %v", err)
	}

	ts.db, err = cluster.NewDatabase(ts.hostID, topo, topoWatch, ts.storageOpts)
	if err != nil {
		return err
	}

	// Check if clients were closed by stopServer and need to be re-created.
	ts.maybeResetClients()

	go func() {
		if err := openAndServe(
			ts.httpClusterAddr(), ts.tchannelClusterAddr(),
			ts.httpNodeAddr(), ts.tchannelNodeAddr(), ts.httpDebugAddr(),
			ts.db, ts.m3dbClient, ts.storageOpts, ts.doneCh,
		); err != nil {
			select {
			case resultCh <- err:
			default:
			}
		}

		ts.closedCh <- struct{}{}
	}()

	waitFn := ts.waitUntilServerIsUp
	if waitForBootstrap {
		waitFn = ts.waitUntilServerIsBootstrapped
	}
	go func() {
		select {
		case resultCh <- waitFn():
		default:
		}
	}()

	err = <-resultCh
	if err == nil {
		ts.logger.Infof("started server")
	} else {
		ts.logger.Errorf("start server error: %v", err)
	}
	return err
}

func (ts *testSetup) stopServer() error {
	ts.doneCh <- struct{}{}

	if ts.m3dbClient.DefaultSessionActive() {
		session, err := ts.m3dbClient.DefaultSession()
		if err != nil {
			return err
		}
		ts.m3dbClient = nil
		ts.m3dbAdminClient = nil
		ts.m3dbVerificationAdminClient = nil
		defer session.Close()
	}

	if err := ts.waitUntilServerIsDown(); err != nil {
		return err
	}

	// Wait for graceful server close
	<-ts.closedCh
	return nil
}

func (ts *testSetup) writeBatch(namespace ident.ID, seriesList generate.SeriesBlock) error {
	if ts.opts.UseTChannelClientForWriting() {
		return tchannelClientWriteBatch(ts.tchannelClient, ts.opts.WriteRequestTimeout(), namespace, seriesList)
	}
	return m3dbClientWriteBatch(ts.m3dbClient, ts.workerPool, namespace, seriesList)
}

func (ts *testSetup) fetch(req *rpc.FetchRequest) ([]ts.Datapoint, error) {
	if ts.opts.UseTChannelClientForReading() {
		return tchannelClientFetch(ts.tchannelClient, ts.opts.ReadRequestTimeout(), req)
	}
	return m3dbClientFetch(ts.m3dbClient, req)
}

func (ts *testSetup) truncate(req *rpc.TruncateRequest) (int64, error) {
	if ts.opts.UseTChannelClientForTruncation() {
		return tchannelClientTruncate(ts.tchannelClient, ts.opts.TruncateRequestTimeout(), req)
	}
	return m3dbClientTruncate(ts.m3dbClient, req)
}

func (ts *testSetup) health() (*rpc.NodeHealthResult_, error) {
	return tchannelClientHealth(ts.tchannelClient)
}

func (ts *testSetup) close() {
	if ts.channel != nil {
		ts.channel.Close()
	}
	if ts.filePathPrefix != "" {
		os.RemoveAll(ts.filePathPrefix)
	}
}

func (ts *testSetup) mustSetTickMinimumInterval(tickMinInterval time.Duration) {
	runtimeMgr := ts.storageOpts.RuntimeOptionsManager()
	existingOptions := runtimeMgr.Get()
	newOptions := existingOptions.SetTickMinimumInterval(tickMinInterval)
	err := runtimeMgr.Update(newOptions)
	if err != nil {
		panic(fmt.Sprintf("err setting tick minimum interval: %v", err))
	}
}

// convenience wrapper used to ensure a tick occurs
func (ts *testSetup) sleepFor10xTickMinimumInterval() {
	// Check the runtime options manager instead of relying on ts.opts
	// because the tick interval can change at runtime.
	runtimeMgr := ts.storageOpts.RuntimeOptionsManager()
	opts := runtimeMgr.Get()
	time.Sleep(opts.TickMinimumInterval() * 10)
}

func (ts *testSetup) httpClusterAddr() string {
	if addr := ts.opts.HTTPClusterAddr(); addr != "" {
		return addr
	}
	return *httpClusterAddr
}

func (ts *testSetup) httpNodeAddr() string {
	if addr := ts.opts.HTTPNodeAddr(); addr != "" {
		return addr
	}
	return *httpNodeAddr
}

func (ts *testSetup) tchannelClusterAddr() string {
	if addr := ts.opts.TChannelClusterAddr(); addr != "" {
		return addr
	}
	return *tchannelClusterAddr
}

func (ts *testSetup) tchannelNodeAddr() string {
	if addr := ts.opts.TChannelNodeAddr(); addr != "" {
		return addr
	}
	return *tchannelNodeAddr
}

func (ts *testSetup) httpDebugAddr() string {
	if addr := ts.opts.HTTPDebugAddr(); addr != "" {
		return addr
	}
	return *httpDebugAddr
}

func (ts *testSetup) maybeResetClients() error {
	if ts.m3dbClient == nil {
		// Recreate the clients as their session was destroyed by stopServer()
		adminClient, verificationAdminClient, err := newClients(
			ts.topoInit, ts.opts, ts.hostID, ts.tchannelNodeAddr())
		if err != nil {
			return err
		}
		ts.m3dbClient = adminClient.(client.Client)
		ts.m3dbAdminClient = adminClient
		ts.m3dbVerificationAdminClient = verificationAdminClient
	}

	return nil
}

// Implements topology.MapProvider, and makes sure that the topology
// map provided always comes from the most recent database in the testSetup
// since they get\ recreated everytime startServer/stopServer is called and
// are not available (nil value) after creation but before the first call
// to startServer.
func (ts *testSetup) TopologyMap() (topology.Map, error) {
	return ts.db.TopologyMap()
}

func newOrigin(id string, tchannelNodeAddr string) topology.Host {
	return topology.NewHost(id, tchannelNodeAddr)
}

func newClients(
	topoInit topology.Initializer,
	opts testOptions,
	id,
	tchannelNodeAddr string,
) (client.AdminClient, client.AdminClient, error) {
	var (
		clientOpts = defaultClientOptions(topoInit).
				SetClusterConnectTimeout(opts.ClusterConnectionTimeout()).
				SetWriteConsistencyLevel(opts.WriteConsistencyLevel()).
				SetTopologyInitializer(topoInit)

		origin             = newOrigin(id, tchannelNodeAddr)
		verificationOrigin = newOrigin(id+"-verification", tchannelNodeAddr)

		adminOpts             = clientOpts.(client.AdminOptions).SetOrigin(origin)
		verificationAdminOpts = adminOpts.SetOrigin(verificationOrigin)
	)

	// Set up m3db client
	adminClient, err := m3dbAdminClient(adminOpts)
	if err != nil {
		return nil, nil, err
	}

	// Set up m3db verification client
	verificationAdminClient, err := m3dbAdminClient(verificationAdminOpts)
	if err != nil {
		return nil, nil, err
	}

	return adminClient, verificationAdminClient, nil
}

type testSetups []*testSetup

func (ts testSetups) parallel(fn func(s *testSetup)) {
	var wg sync.WaitGroup
	for _, setup := range ts {
		s := setup
		wg.Add(1)
		go func() {
			fn(s)
			wg.Done()
		}()
	}
	wg.Wait()
}

// node generates service instances with reasonable defaults
func node(t *testing.T, n int, shards shard.Shards) services.ServiceInstance {
	require.True(t, n < 250) // keep ports sensible
	return services.NewServiceInstance().
		SetInstanceID(fmt.Sprintf("testhost%v", n)).
		SetEndpoint(fmt.Sprintf("127.0.0.1:%v", multiAddrPortStart+multiAddrPortEach*n)).
		SetShards(shards)
}

// newNodes creates a set of testSetups with reasonable defaults
func newNodes(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
	nspaces []namespace.Metadata,
	asyncInserts bool,
) (testSetups, topology.Initializer, closeFn) {
	var (
		log  = xlog.SimpleLogger
		opts = newTestOptions(t).
			SetNamespaces(nspaces).
			SetTickMinimumInterval(3 * time.Second).
			SetWriteNewSeriesAsync(asyncInserts).
			SetNumShards(numShards)

		// NB(bl): We set replication to 3 to mimic production. This can be made
		// into a variable if needed.
		svc = fake.NewM3ClusterService().
			SetInstances(instances).
			SetReplication(services.NewServiceReplication().SetReplicas(3)).
			SetSharding(services.NewServiceSharding().SetNumShards(numShards))

		svcs = fake.NewM3ClusterServices()
	)
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)

	nodeOpt := bootstrappableTestSetupOptions{
		disablePeersBootstrapper: true,
		finalBootstrapper:        bootstrapper.NoOpAllBootstrapperName,
		topologyInitializer:      topoInit,
	}

	nodeOpts := make([]bootstrappableTestSetupOptions, len(instances))
	for i := range instances {
		nodeOpts[i] = nodeOpt
	}

	nodes, closeFn := newDefaultBootstrappableTestSetups(t, opts, nodeOpts)

	nodeClose := func() { // Clean up running servers at end of test
		log.Debug("servers closing")
		nodes.parallel(func(s *testSetup) {
			if s.serverIsBootstrapped() {
				require.NoError(t, s.stopServer())
			}
		})
		closeFn()
		log.Debug("servers are now down")
	}

	return nodes, topoInit, nodeClose
}
