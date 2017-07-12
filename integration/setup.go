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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/integration/fake"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/cluster"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/require"
	tchannel "github.com/uber/tchannel-go"
)

var (
	id                  = flag.String("id", "", "Node host ID")
	httpClusterAddr     = flag.String("clusterhttpaddr", "0.0.0.0:9000", "Cluster HTTP server address")
	tchannelClusterAddr = flag.String("clustertchanneladdr", "0.0.0.0:9001", "Cluster TChannel server address")
	httpNodeAddr        = flag.String("nodehttpaddr", "0.0.0.0:9002", "Node HTTP server address")
	tchannelNodeAddr    = flag.String("nodetchanneladdr", "0.0.0.0:9003", "Node TChannel server address")

	errServerStartTimedOut   = errors.New("server took too long to start")
	errServerStopTimedOut    = errors.New("server took too long to stop")
	testNamespaces           = []ts.ID{ts.StringID("testNs1"), ts.StringID("testNs2")}
	testNativePoolingBuckets = []pool.Bucket{{Capacity: 4096, Count: 256}}

	created = uint64(0)
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

type testSetup struct {
	t               *testing.T
	opts            testOptions
	db              cluster.Database
	storageOpts     storage.Options
	fsOpts          fs.Options
	nativePooling   bool
	hostID          string
	topoInit        topology.Initializer
	shardSet        sharding.ShardSet
	getNowFn        clock.NowFn
	setNowFn        nowSetterFn
	tchannelClient  rpc.TChanNode
	m3dbClient      client.Client
	m3dbAdminClient client.AdminClient
	workerPool      xsync.WorkerPool

	// things that need to be cleaned up
	channel        *tchannel.Channel
	filePathPrefix string
	namespaces     []namespace.Metadata

	// signals
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestSetup(t *testing.T, opts testOptions) (*testSetup, error) {
	if opts == nil {
		opts = newTestOptions(t)
	}

	storageOpts := storage.NewOptions().
		SetNamespaceInitializer(namespace.NewStaticInitializer(opts.Namespaces())).
		SetTickInterval(opts.TickInterval())

	nativePooling := strings.ToLower(os.Getenv("TEST_NATIVE_POOLING")) == "true"
	if nativePooling {
		buckets := testNativePoolingBuckets
		bytesPool := pool.NewCheckedBytesPool(buckets, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewNativeHeap(s, nil)
		})
		bytesPool.Init()

		storageOpts = storageOpts.SetBytesPool(bytesPool)

		idPool := ts.NewNativeIdentifierPool(bytesPool, nil)

		storageOpts = storageOpts.SetIdentifierPool(idPool)
	}

	// Set up shard set
	shardSet, err := server.DefaultShardSet()
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
		topoInit, err = server.DefaultTopologyInitializerForShardSet(id, tchannelNodeAddr, shardSet)
		if err != nil {
			return nil, err
		}
	}

	clientOpts := server.DefaultClientOptions(topoInit).
		SetClusterConnectTimeout(opts.ClusterConnectionTimeout()).
		SetWriteConsistencyLevel(opts.WriteConsistencyLevel())

	adminOpts, ok := clientOpts.(client.AdminOptions)
	if !ok {
		return nil, fmt.Errorf("unable to cast to admin options")
	}

	// Set up tchannel client
	channel, tc, err := tchannelClient(tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up m3db client
	adminClient, err := m3dbAdminClient(adminOpts)
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
		storageOpts.InstrumentOptions().Logger().Warnf(
			"Unable to find a single blockSize from known retention periods, guessing: %v",
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
	storageOpts = storageOpts.SetClockOptions(storageOpts.ClockOptions().SetNowFn(getNowFn))

	// Set up file path prefix
	idx := atomic.AddUint64(&created, 1) - 1
	filePathPrefix, err := ioutil.TempDir("", fmt.Sprintf("integration-test-%d", idx))
	if err != nil {
		return nil, err
	}

	fsOpts := fs.NewOptions().
		SetFilePathPrefix(filePathPrefix)

	storageOpts = storageOpts.SetCommitLogOptions(
		storageOpts.CommitLogOptions().
			SetFilesystemOptions(fsOpts).
			SetRetentionOptions(opts.CommitLogRetention()))

	// Set up persistence manager
	storageOpts = storageOpts.SetPersistManager(fs.NewPersistManager(fsOpts))

	// Set up repair options
	storageOpts = storageOpts.SetRepairOptions(storageOpts.RepairOptions().SetAdminClient(adminClient))

	// Set up block retriever manager
	if mgr := opts.DatabaseBlockRetrieverManager(); mgr != nil {
		storageOpts = storageOpts.SetDatabaseBlockRetrieverManager(mgr)
	}

	return &testSetup{
		t:               t,
		opts:            opts,
		storageOpts:     storageOpts,
		fsOpts:          fsOpts,
		nativePooling:   nativePooling,
		hostID:          id,
		topoInit:        topoInit,
		shardSet:        shardSet,
		getNowFn:        getNowFn,
		setNowFn:        setNowFn,
		tchannelClient:  tc,
		m3dbClient:      adminClient.(client.Client),
		m3dbAdminClient: adminClient,
		workerPool:      workerPool,
		channel:         channel,
		filePathPrefix:  filePathPrefix,
		namespaces:      opts.Namespaces(),
		doneCh:          make(chan struct{}),
		closedCh:        make(chan struct{}),
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
		bs := md.Options().RetentionOptions().BlockSize().Nanoseconds() / 1000 / 1000
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

func (ts *testSetup) namespaceMetadataOrFail(id ts.ID) namespace.Metadata {
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

func (ts *testSetup) serverIsUp() bool {
	resp, err := ts.health()
	return err == nil && resp.Bootstrapped
}

func (ts *testSetup) serverIsDown() bool {
	_, err := ts.health()
	return err != nil
}

func (ts *testSetup) waitUntilServerIsUp() error {
	if waitUntil(ts.serverIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	if waitUntil(ts.serverIsDown, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServer() error {
	log := ts.storageOpts.InstrumentOptions().Logger()
	fields := []xlog.LogField{
		xlog.NewLogField("nativepooling", ts.nativePooling),
	}
	log.WithFields(fields...).Infof("starting server")

	resultCh := make(chan error, 1)

	httpClusterAddr := *httpClusterAddr
	if addr := ts.opts.HTTPClusterAddr(); addr != "" {
		httpClusterAddr = addr
	}

	tchannelClusterAddr := *tchannelClusterAddr
	if addr := ts.opts.TChannelClusterAddr(); addr != "" {
		tchannelClusterAddr = addr
	}

	httpNodeAddr := *httpNodeAddr
	if addr := ts.opts.HTTPNodeAddr(); addr != "" {
		httpNodeAddr = addr
	}

	tchannelNodeAddr := *tchannelNodeAddr
	if addr := ts.opts.TChannelNodeAddr(); addr != "" {
		tchannelNodeAddr = addr
	}

	var err error
	ts.db, err = cluster.NewDatabase(ts.hostID, ts.topoInit, ts.storageOpts)
	if err != nil {
		return err
	}
	go func() {
		if err := server.OpenAndServe(
			httpClusterAddr, tchannelClusterAddr,
			httpNodeAddr, tchannelNodeAddr,
			ts.db, ts.m3dbClient, ts.storageOpts, ts.doneCh,
		); err != nil {
			select {
			case resultCh <- err:
			default:
			}
		}

		ts.closedCh <- struct{}{}
	}()

	go func() {
		select {
		case resultCh <- ts.waitUntilServerIsUp():
		default:
		}
	}()

	err = <-resultCh
	if err == nil {
		log.WithFields(fields...).Infof("started server")
	} else {
		log.WithFields(fields...).Errorf("start server error: %v", err)
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
		defer session.Close()
	}

	if err := ts.waitUntilServerIsDown(); err != nil {
		return err
	}

	// Wait for graceful server close
	<-ts.closedCh
	return nil
}

func (ts *testSetup) writeBatch(namespace ts.ID, seriesList generate.SeriesBlock) error {
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

type testSetups []*testSetup

func (ts testSetups) parallel(fn func(s *testSetup)) {
	var wg sync.WaitGroup
	for _, setup := range ts {
		setup := setup
		wg.Add(1)
		go func() {
			fn(setup)
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
		SetEndpoint(fmt.Sprintf("127.0.0.1:%v", 9000+4*n)).
		SetShards(shards)
}

// newNodes creates a set of testSetups with reasonable defaults
func newNodes(
	t *testing.T,
	instances []services.ServiceInstance,
	nspaces []namespace.Metadata,
) (testSetups, topology.Initializer, closeFn) {

	log := xlog.SimpleLogger
	opts := newTestOptions(t).
		SetNamespaces(nspaces).
		SetTickInterval(3 * time.Second)

	// NB(bl): We set replication to 3 to mimic production. This can be made
	// into a variable if needed.
	svc := fake.NewM3ClusterService().
		SetInstances(instances).
		SetReplication(services.NewServiceReplication().SetReplicas(3)).
		SetSharding(services.NewServiceSharding().SetNumShards(1024))

	svcs := fake.NewM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)

	nodeOpt := bootstrappableTestSetupOptions{
		disablePeersBootstrapper: true,
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
			if s.serverIsUp() {
				require.NoError(t, s.stopServer())
			}
		})
		log.Debug("servers are now down")
		closeFn()
	}

	return nodes, topoInit, nodeClose
}
