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
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration/fake"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/server"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	bfs "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/cluster"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

	testSchemaHistory = prototest.NewSchemaHistory()
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoIter     = prototest.NewProtoMessageIterator(testProtoMessages)
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

type assertTestDataEqual func(t *testing.T, expected, actual []generate.TestValue) bool

var _ topology.MapProvider = &testSetup{}

type testSetup struct {
	t         *testing.T
	opts      TestOptions
	schemaReg namespace.SchemaRegistry

	logger *zap.Logger
	scope  tally.TestScope

	db                cluster.Database
	storageOpts       storage.Options
	serverStorageOpts server.StorageOptions
	fsOpts            fs.Options
	blockLeaseManager block.LeaseManager
	hostID            string
	origin            topology.Host
	topoInit          topology.Initializer
	shardSet          sharding.ShardSet
	getNowFn          clock.NowFn
	setNowFn          nowSetterFn
	tchannelClient    *TestTChannelClient
	m3dbClient        client.Client
	// We need two distinct clients where one has the origin set to the same ID as the
	// node itself (I.E) the client will behave exactly as if it is the node itself
	// making requests, and another client with the origin set to an ID different than
	// the node itself so that we can make requests from the perspective of a "different"
	// M3DB node for verification purposes in some of the tests.
	m3dbAdminClient             client.AdminClient
	m3dbVerificationAdminClient client.AdminClient
	workerPool                  xsync.WorkerPool

	// compare expected with actual data function
	assertEqual assertTestDataEqual

	// things that need to be cleaned up
	channel        *tchannel.Channel
	filePathPrefix string
	namespaces     []namespace.Metadata

	// signals
	doneCh chan struct {
	}
	closedCh chan struct {
	}
}

// TestSetup is a test setup.
type TestSetup interface {
	topology.MapProvider

	Opts() TestOptions
	SetOpts(TestOptions)
	FilesystemOpts() fs.Options
	AssertEqual(*testing.T, []generate.TestValue, []generate.TestValue) bool
	DB() cluster.Database
	Scope() tally.TestScope
	M3DBClient() client.Client
	M3DBVerificationAdminClient() client.AdminClient
	TChannelClient() *TestTChannelClient
	Namespaces() []namespace.Metadata
	TopologyInitializer() topology.Initializer
	SetTopologyInitializer(topology.Initializer)
	Fetch(req *rpc.FetchRequest) ([]generate.TestValue, error)
	FilePathPrefix() string
	StorageOpts() storage.Options
	SetStorageOpts(storage.Options)
	SetServerStorageOpts(server.StorageOptions)
	Origin() topology.Host
	ServerIsBootstrapped() bool
	StopServer() error
	StartServer() error
	StartServerDontWaitBootstrap() error
	NowFn() clock.NowFn
	SetNowFn(time.Time)
	Close()
	WriteBatch(ident.ID, generate.SeriesBlock) error
	ShouldBeEqual() bool
	// *NOTE*: This method is deprecated and should not be used in future tests.
	// Also, we should migrate existing tests when we touch them away from using this.
	SleepFor10xTickMinimumInterval()
	BlockLeaseManager() block.LeaseManager
	ShardSet() sharding.ShardSet
	SetShardSet(sharding.ShardSet)
	GeneratorOptions(retention.Options) generate.Options
	MaybeResetClients() error
	SchemaRegistry() namespace.SchemaRegistry
	NamespaceMetadataOrFail(ident.ID) namespace.Metadata
	MustSetTickMinimumInterval(time.Duration)
	WaitUntilServerIsBootstrapped() error
	WaitUntilServerIsUp() error
	WaitUntilServerIsDown() error
	Truncate(*rpc.TruncateRequest) (int64, error)
	InitializeBootstrappers(opts InitializeBootstrappersOptions) error
}

type storageOption func(storage.Options) storage.Options

// NewTestSetup returns a new test setup for non-dockerized integration tests.
func NewTestSetup(
	t *testing.T,
	opts TestOptions,
	fsOpts fs.Options,
	storageOptFns ...storageOption,
) (TestSetup, error) {
	if opts == nil {
		opts = NewTestOptions(t)
	}

	nsInit := opts.NamespaceInitializer()
	if nsInit == nil {
		nsInit = namespace.NewStaticInitializer(opts.Namespaces())
	}

	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.DisableCaller = true
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		var parsedLevel zap.AtomicLevel
		if err := parsedLevel.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("unable to parse log level: %v", err)
		}
		zapConfig.Level = parsedLevel
	}
	logger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}

	// Schema registry is shared between database and admin client.
	schemaReg := namespace.NewSchemaRegistry(opts.ProtoEncoding(), nil)

	blockLeaseManager := block.NewLeaseManager(nil)
	storageOpts := storage.NewOptions().
		SetNamespaceInitializer(nsInit).
		SetSchemaRegistry(schemaReg).
		SetBlockLeaseManager(blockLeaseManager)

	if opts.ProtoEncoding() {
		blockOpts := storageOpts.DatabaseBlockOptions().
			SetEncoderPool(prototest.ProtoPools.EncoderPool).
			SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
			SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)
		storageOpts = storageOpts.
			SetDatabaseBlockOptions(blockOpts).
			SetEncoderPool(prototest.ProtoPools.EncoderPool).
			SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
			SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)
	}

	if strings.ToLower(os.Getenv("TEST_DEBUG_LOG")) == "true" {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		logger, err = zapConfig.Build()
		if err != nil {
			return nil, err
		}
		storageOpts = storageOpts.SetInstrumentOptions(
			storageOpts.InstrumentOptions().SetLogger(logger))
	}

	scope := tally.NewTestScope("", nil)
	storageOpts = storageOpts.SetInstrumentOptions(
		storageOpts.InstrumentOptions().SetMetricsScope(scope))

	// Use specified series cache policy from environment if set.
	seriesCachePolicy := strings.ToLower(os.Getenv("TEST_SERIES_CACHE_POLICY"))
	if seriesCachePolicy != "" {
		value, err := series.ParseCachePolicy(seriesCachePolicy)
		if err != nil {
			return nil, err
		}
		storageOpts = storageOpts.SetSeriesCachePolicy(value)
	}

	fields := []zapcore.Field{
		zap.Stringer("cache-policy", storageOpts.SeriesCachePolicy()),
	}
	logger = logger.With(fields...)
	iOpts := storageOpts.InstrumentOptions()
	storageOpts = storageOpts.SetInstrumentOptions(iOpts.SetLogger(logger))

	indexMode := index.InsertSync
	if opts.WriteNewSeriesAsync() {
		indexMode = index.InsertAsync
	}

	plCache, stopReporting, err := index.NewPostingsListCache(10, index.PostingsListCacheOptions{
		InstrumentOptions: iOpts,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create postings list cache: %v", err)
	}
	// Ok to run immediately since it just closes the background reporting loop. Only ok because
	// this is a test setup, in production we would want the metrics.
	stopReporting()

	indexOpts := storageOpts.IndexOptions().
		SetInsertMode(indexMode).
		SetPostingsListCache(plCache)
	storageOpts = storageOpts.SetIndexOptions(indexOpts)

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

	adminClient, verificationAdminClient, err := newClients(topoInit, opts, schemaReg, id, tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up tchannel client
	tchanClient, err := NewTChannelClient("integration-test", tchannelNodeAddr)
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
		logger.Warn("unable to find a single blockSize from known retention periods",
			zap.String("guessing", truncateSize.String()))
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
	if overrideTimeNow := opts.NowFn(); overrideTimeNow != nil {
		// Allow overriding the frozen time
		storageOpts = storageOpts.SetClockOptions(
			storageOpts.ClockOptions().SetNowFn(overrideTimeNow))
	} else {
		storageOpts = storageOpts.SetClockOptions(
			storageOpts.ClockOptions().SetNowFn(getNowFn))
	}

	// Set up file path prefix
	filePathPrefix := opts.FilePathPrefix()
	if filePathPrefix == "" {
		var err error
		filePathPrefix, err = ioutil.TempDir("", "integration-test")
		if err != nil {
			return nil, err
		}
	}

	if fsOpts == nil {
		fsOpts = fs.NewOptions().
			SetFilePathPrefix(filePathPrefix).
			SetClockOptions(storageOpts.ClockOptions())
	}

	storageOpts = storageOpts.SetCommitLogOptions(
		storageOpts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	// Set up persistence manager
	pm, err := fs.NewPersistManager(fsOpts)
	if err != nil {
		return nil, err
	}
	storageOpts = storageOpts.SetPersistManager(pm)

	// Set up index claims manager
	icm, err := fs.NewIndexClaimsManager(fsOpts)
	if err != nil {
		return nil, err
	}
	storageOpts = storageOpts.SetIndexClaimsManager(icm)

	// Set up repair options
	storageOpts = storageOpts.
		SetRepairOptions(storageOpts.RepairOptions().
			SetAdminClients([]client.AdminClient{adminClient}))

	// Set up block retriever manager
	if mgr := opts.DatabaseBlockRetrieverManager(); mgr != nil {
		storageOpts = storageOpts.SetDatabaseBlockRetrieverManager(mgr)
	} else {
		switch storageOpts.SeriesCachePolicy() {
		case series.CacheAll:
			// Do not need a block retriever for CacheAll policy
		default:
			blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
				func(md namespace.Metadata, shardSet sharding.ShardSet) (block.DatabaseBlockRetriever, error) {
					retrieverOpts := fs.NewBlockRetrieverOptions().
						SetBlockLeaseManager(blockLeaseManager)
					retriever, err := fs.NewBlockRetriever(retrieverOpts, fsOpts)
					if err != nil {
						return nil, err
					}

					if err := retriever.Open(md, shardSet); err != nil {
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
			return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts, namespace.Context{})
		})
		blockOpts = blockOpts.SetDatabaseBlockPool(blockPool)
		storageOpts = storageOpts.SetDatabaseBlockOptions(blockOpts)
	}

	storageOpts = storageOpts.SetInstrumentOptions(
		storageOpts.InstrumentOptions().SetReportInterval(opts.ReportInterval()))

	// Set debugging options if environment vars set
	if debugFilePrefix := os.Getenv("TEST_DEBUG_FILE_PREFIX"); debugFilePrefix != "" {
		opts = opts.SetVerifySeriesDebugFilePathPrefix(debugFilePrefix)
	}

	for _, fn := range storageOptFns {
		storageOpts = fn(storageOpts)
	}

	return &testSetup{
		t:                           t,
		opts:                        opts,
		schemaReg:                   schemaReg,
		logger:                      logger,
		scope:                       scope,
		storageOpts:                 storageOpts,
		blockLeaseManager:           blockLeaseManager,
		fsOpts:                      fsOpts,
		hostID:                      id,
		origin:                      newOrigin(id, tchannelNodeAddr),
		topoInit:                    topoInit,
		shardSet:                    shardSet,
		getNowFn:                    getNowFn,
		setNowFn:                    setNowFn,
		tchannelClient:              tchanClient,
		m3dbClient:                  adminClient.(client.Client),
		m3dbAdminClient:             adminClient,
		m3dbVerificationAdminClient: verificationAdminClient,
		workerPool:                  workerPool,
		filePathPrefix:              filePathPrefix,
		namespaces:                  opts.Namespaces(),
		doneCh:                      make(chan struct{}),
		closedCh:                    make(chan struct{}),
		assertEqual:                 opts.AssertTestDataEqual(),
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

func (ts *testSetup) ShouldBeEqual() bool {
	return ts.assertEqual == nil
}

func (ts *testSetup) AssertEqual(t *testing.T, a, b []generate.TestValue) bool {
	return ts.assertEqual(t, a, b)
}

func (ts *testSetup) DB() cluster.Database {
	return ts.db
}

func (ts *testSetup) Scope() tally.TestScope {
	return ts.scope
}

func (ts *testSetup) M3DBClient() client.Client {
	return ts.m3dbClient
}

func (ts *testSetup) M3DBVerificationAdminClient() client.AdminClient {
	return ts.m3dbVerificationAdminClient
}

func (ts *testSetup) Namespaces() []namespace.Metadata {
	return ts.namespaces
}

func (ts *testSetup) NowFn() clock.NowFn {
	return ts.getNowFn
}

func (ts *testSetup) SetNowFn(t time.Time) {
	ts.setNowFn(t)
}

func (ts *testSetup) FilesystemOpts() fs.Options {
	return ts.fsOpts
}

func (ts *testSetup) Opts() TestOptions {
	return ts.opts
}

func (ts *testSetup) SetOpts(opts TestOptions) {
	ts.opts = opts
}

func (ts *testSetup) Origin() topology.Host {
	return ts.origin
}

func (ts *testSetup) FilePathPrefix() string {
	return ts.filePathPrefix
}

func (ts *testSetup) StorageOpts() storage.Options {
	return ts.storageOpts
}

func (ts *testSetup) SetStorageOpts(opts storage.Options) {
	ts.storageOpts = opts
}

func (ts *testSetup) SetServerStorageOpts(opts server.StorageOptions) {
	ts.serverStorageOpts = opts
}

func (ts *testSetup) TopologyInitializer() topology.Initializer {
	return ts.topoInit
}

func (ts *testSetup) SetTopologyInitializer(init topology.Initializer) {
	ts.topoInit = init
}

func (ts *testSetup) BlockLeaseManager() block.LeaseManager {
	return ts.blockLeaseManager
}

func (ts *testSetup) ShardSet() sharding.ShardSet {
	return ts.shardSet
}

func (ts *testSetup) SetShardSet(shardSet sharding.ShardSet) {
	ts.shardSet = shardSet
}

func (ts *testSetup) NamespaceMetadataOrFail(id ident.ID) namespace.Metadata {
	for _, md := range ts.namespaces {
		if md.ID().Equal(id) {
			return md
		}
	}
	require.FailNow(ts.t, "unable to find namespace", id.String())
	return nil
}

func (ts *testSetup) GeneratorOptions(ropts retention.Options) generate.Options {
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

func (ts *testSetup) ServerIsBootstrapped() bool {
	resp, err := ts.health()
	return err == nil && resp.Bootstrapped
}

func (ts *testSetup) ServerIsUp() bool {
	_, err := ts.health()
	return err == nil
}

func (ts *testSetup) ServerIsDown() bool {
	return !ts.ServerIsUp()
}

func (ts *testSetup) WaitUntilServerIsBootstrapped() error {
	if waitUntil(ts.ServerIsBootstrapped, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) WaitUntilServerIsUp() error {
	if waitUntil(ts.ServerIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) WaitUntilServerIsDown() error {
	if waitUntil(ts.ServerIsDown, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) StartServerDontWaitBootstrap() error {
	return ts.startServerBase(false)
}

func (ts *testSetup) StartServer() error {
	return ts.startServerBase(true)
}

func (ts *testSetup) startServerBase(waitForBootstrap bool) error {
	ts.logger.Info("starting server")

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

	leaseVerifier := storage.NewLeaseVerifier(ts.db)
	if err := ts.blockLeaseManager.SetLeaseVerifier(leaseVerifier); err != nil {
		return err
	}

	// Check if clients were closed by StopServer and need to be re-created.
	ts.MaybeResetClients()

	go func() {
		if err := openAndServe(
			ts.httpClusterAddr(), ts.tchannelClusterAddr(),
			ts.httpNodeAddr(), ts.tchannelNodeAddr(), ts.httpDebugAddr(),
			ts.db, ts.m3dbClient, ts.storageOpts, ts.serverStorageOpts, ts.doneCh,
		); err != nil {
			select {
			case resultCh <- err:
			default:
			}
		}

		ts.closedCh <- struct{}{}
	}()

	waitFn := ts.WaitUntilServerIsUp
	if waitForBootstrap {
		waitFn = ts.WaitUntilServerIsBootstrapped
	}
	go func() {
		select {
		case resultCh <- waitFn():
		default:
		}
	}()

	err = <-resultCh
	if err == nil {
		ts.logger.Info("started server")
	} else {
		ts.logger.Error("start server error", zap.Error(err))
	}
	return err
}

func (ts *testSetup) StopServer() error {
	ts.doneCh <- struct{}{}

	// NB(bodu): Need to reset the global counter of index claims managers after
	// we've stopped the test server. This covers the restart server case.
	fs.ResetIndexClaimsManagersUnsafe()

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

	if err := ts.WaitUntilServerIsDown(); err != nil {
		return err
	}

	// Wait for graceful server close
	<-ts.closedCh
	return nil
}

func (ts *testSetup) TChannelClient() *TestTChannelClient {
	return ts.tchannelClient
}

func (ts *testSetup) WriteBatch(namespace ident.ID, seriesList generate.SeriesBlock) error {
	if ts.opts.UseTChannelClientForWriting() {
		return ts.tchannelClient.TChannelClientWriteBatch(
			ts.opts.WriteRequestTimeout(), namespace, seriesList)
	}
	return m3dbClientWriteBatch(ts.m3dbClient, ts.workerPool, namespace, seriesList)
}

func (ts *testSetup) Fetch(req *rpc.FetchRequest) ([]generate.TestValue, error) {
	if ts.opts.UseTChannelClientForReading() {
		fetched, err := ts.tchannelClient.TChannelClientFetch(ts.opts.ReadRequestTimeout(), req)
		if err != nil {
			return nil, err
		}
		dp := toDatapoints(fetched)
		return dp, nil
	}
	return m3dbClientFetch(ts.m3dbClient, req)
}

func (ts *testSetup) Truncate(req *rpc.TruncateRequest) (int64, error) {
	if ts.opts.UseTChannelClientForTruncation() {
		return ts.tchannelClient.TChannelClientTruncate(ts.opts.TruncateRequestTimeout(), req)
	}
	return m3dbClientTruncate(ts.m3dbClient, req)
}

func (ts *testSetup) health() (*rpc.NodeHealthResult_, error) {
	return ts.tchannelClient.TChannelClientHealth(5 * time.Second)
}

func (ts *testSetup) Close() {
	if ts.channel != nil {
		ts.channel.Close()
	}
	if ts.filePathPrefix != "" {
		os.RemoveAll(ts.filePathPrefix)
	}

	// This could get called more than once in the multi node integration test case
	// but this is fine since the reset always sets the counter to 0.
	fs.ResetIndexClaimsManagersUnsafe()
}

func (ts *testSetup) MustSetTickMinimumInterval(tickMinInterval time.Duration) {
	runtimeMgr := ts.storageOpts.RuntimeOptionsManager()
	existingOptions := runtimeMgr.Get()
	newOptions := existingOptions.SetTickMinimumInterval(tickMinInterval)
	err := runtimeMgr.Update(newOptions)
	if err != nil {
		panic(fmt.Sprintf("err setting tick minimum interval: %v", err))
	}
}

// convenience wrapper used to ensure a tick occurs
func (ts *testSetup) SleepFor10xTickMinimumInterval() {
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

func (ts *testSetup) MaybeResetClients() error {
	if ts.m3dbClient == nil {
		// Recreate the clients as their session was destroyed by StopServer()
		adminClient, verificationAdminClient, err := newClients(
			ts.topoInit, ts.opts, ts.schemaReg, ts.hostID, ts.tchannelNodeAddr())
		if err != nil {
			return err
		}
		ts.m3dbClient = adminClient.(client.Client)
		ts.m3dbAdminClient = adminClient
		ts.m3dbVerificationAdminClient = verificationAdminClient
	}

	return nil
}

func (ts *testSetup) SchemaRegistry() namespace.SchemaRegistry {
	return ts.schemaReg
}

// InitializeBootstrappersOptions supplies options for bootstrapper initialization.
type InitializeBootstrappersOptions struct {
	CommitLogOptions commitlog.Options
	WithCommitLog    bool
	WithFileSystem   bool
}

func (o InitializeBootstrappersOptions) validate() error {
	if o.WithCommitLog && o.CommitLogOptions == nil {
		return errors.New("commit log options required when initializing a commit log bootstrapper")
	}
	return nil
}

func (ts *testSetup) InitializeBootstrappers(opts InitializeBootstrappersOptions) error {
	var err error
	if err := opts.validate(); err != nil {
		return err
	}

	bs := bootstrapper.NewNoOpAllBootstrapperProvider()
	storageOpts := ts.StorageOpts()
	bsOpts := newDefaulTestResultOptions(storageOpts)
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	if opts.WithCommitLog {
		bclOpts := bcl.NewOptions().
			SetResultOptions(bsOpts).
			SetCommitLogOptions(opts.CommitLogOptions).
			SetRuntimeOptionsManager(runtime.NewOptionsManager())
		bs, err = bcl.NewCommitLogBootstrapperProvider(
			bclOpts, mustInspectFilesystem(fsOpts), bs)
		if err != nil {
			return err
		}
	}

	if opts.WithFileSystem {
		persistMgr, err := fs.NewPersistManager(fsOpts)
		if err != nil {
			return err
		}
		storageIdxOpts := storageOpts.IndexOptions()
		compactor, err := newCompactorWithErr(storageIdxOpts)
		if err != nil {
			return err
		}
		bfsOpts := bfs.NewOptions().
			SetResultOptions(bsOpts).
			SetFilesystemOptions(fsOpts).
			SetIndexOptions(storageIdxOpts).
			SetPersistManager(persistMgr).
			SetIndexClaimsManager(storageOpts.IndexClaimsManager()).
			SetCompactor(compactor)
		bs, err = bfs.NewFileSystemBootstrapperProvider(bfsOpts, bs)
		if err != nil {
			return err
		}
	}

	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(ts).
		SetOrigin(ts.Origin())
	process, err := bootstrap.NewProcessProvider(bs, processOpts, bsOpts, fsOpts)
	if err != nil {
		return err
	}
	ts.SetStorageOpts(storageOpts.SetBootstrapProcessProvider(process))

	return nil
}

// Implements topology.MapProvider, and makes sure that the topology
// map provided always comes from the most recent database in the testSetup
// since they get\ recreated everytime StartServer/StopServer is called and
// are not available (nil value) after creation but before the first call
// to StartServer.
func (ts *testSetup) TopologyMap() (topology.Map, error) {
	return ts.db.TopologyMap()
}

func newOrigin(id string, tchannelNodeAddr string) topology.Host {
	return topology.NewHost(id, tchannelNodeAddr)
}

func newClients(
	topoInit topology.Initializer,
	opts TestOptions,
	schemaReg namespace.SchemaRegistry,
	id,
	tchannelNodeAddr string,
) (client.AdminClient, client.AdminClient, error) {
	var (
		clientOpts = defaultClientOptions(topoInit).SetClusterConnectTimeout(
			opts.ClusterConnectionTimeout()).
			SetFetchRequestTimeout(opts.FetchRequestTimeout()).
			SetWriteConsistencyLevel(opts.WriteConsistencyLevel()).
			SetTopologyInitializer(topoInit).
			SetUseV2BatchAPIs(true)

		origin             = newOrigin(id, tchannelNodeAddr)
		verificationOrigin = newOrigin(id+"-verification", tchannelNodeAddr)

		adminOpts = clientOpts.(client.AdminOptions).SetOrigin(origin).SetSchemaRegistry(schemaReg)

		verificationAdminOpts = adminOpts.SetOrigin(verificationOrigin).SetSchemaRegistry(schemaReg)
	)

	if opts.ProtoEncoding() {
		adminOpts = adminOpts.SetEncodingProto(prototest.ProtoPools.EncodingOpt).(client.AdminOptions)
		verificationAdminOpts = verificationAdminOpts.SetEncodingProto(prototest.ProtoPools.EncodingOpt).(client.AdminOptions)
	}

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

type testSetups []TestSetup

func (ts testSetups) parallel(fn func(s TestSetup)) {
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
		log  = zap.L()
		opts = NewTestOptions(t).
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
		nodes.parallel(func(s TestSetup) {
			if s.ServerIsBootstrapped() {
				require.NoError(t, s.StopServer())
			}
		})
		closeFn()
		log.Debug("servers are now down")
	}

	return nodes, topoInit, nodeClose
}

func mustInspectFilesystem(fsOpts fs.Options) fs.Inspection {
	inspection, err := fs.InspectFilesystem(fsOpts)
	if err != nil {
		panic(err)
	}

	return inspection
}
