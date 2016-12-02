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
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/cluster"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/sync"

	"github.com/uber/tchannel-go"
)

var (
	id                  = flag.String("id", "", "Node host ID")
	httpClusterAddr     = flag.String("clusterhttpaddr", "0.0.0.0:9000", "Cluster HTTP server address")
	tchannelClusterAddr = flag.String("clustertchanneladdr", "0.0.0.0:9001", "Cluster TChannel server address")
	httpNodeAddr        = flag.String("nodehttpaddr", "0.0.0.0:9002", "Node HTTP server address")
	tchannelNodeAddr    = flag.String("nodetchanneladdr", "0.0.0.0:9003", "Node TChannel server address")

	errServerStartTimedOut = errors.New("server took too long to start")
	errServerStopTimedOut  = errors.New("server took too long to stop")
	testNamespaces         = []ts.ID{ts.StringID("testNs1"), ts.StringID("testNs2")}

	created = uint64(0)
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

type testSetup struct {
	opts           testOptions
	db             cluster.Database
	storageOpts    storage.Options
	hostID         string
	topoInit       topology.Initializer
	shardSet       sharding.ShardSet
	getNowFn       clock.NowFn
	setNowFn       nowSetterFn
	tchannelClient rpc.TChanNode
	m3dbClient     client.Client
	workerPool     xsync.WorkerPool

	// things that need to be cleaned up
	channel        *tchannel.Channel
	filePathPrefix string
	namespaces     []namespace.Metadata

	// signals
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestSetup(opts testOptions) (*testSetup, error) {
	if opts == nil {
		opts = newTestOptions()
	}

	storageOpts := storage.NewOptions()

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
		SetClusterConnectTimeout(opts.ClusterConnectionTimeout())

	// Set up tchannel client
	channel, tc, err := tchannelClient(tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up m3db client
	mc, err := m3dbClient(clientOpts)
	if err != nil {
		return nil, err
	}

	// Set up worker pool
	workerPool := xsync.NewWorkerPool(opts.WorkerPoolSize())
	workerPool.Init()

	// Set up getter and setter for now
	var lock sync.RWMutex
	now := time.Now().Truncate(storageOpts.RetentionOptions().BlockSize())
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

	fsOpts := fs.NewOptions().SetFilePathPrefix(filePathPrefix)

	storageOpts = storageOpts.SetCommitLogOptions(storageOpts.CommitLogOptions().SetFilesystemOptions(fsOpts))

	// Set up persistence manager
	storageOpts = storageOpts.SetNewPersistManagerFn(func() persist.Manager {
		return fs.NewPersistManager(fsOpts)
	})

	// Set up repair options
	storageOpts = storageOpts.SetRepairOptions(storageOpts.RepairOptions().SetAdminClient(mc.(client.AdminClient)))

	return &testSetup{
		opts:           opts,
		storageOpts:    storageOpts,
		hostID:         id,
		topoInit:       topoInit,
		shardSet:       shardSet,
		getNowFn:       getNowFn,
		setNowFn:       setNowFn,
		tchannelClient: tc,
		m3dbClient:     mc,
		workerPool:     workerPool,
		channel:        channel,
		filePathPrefix: filePathPrefix,
		namespaces:     opts.Namespaces(),
		doneCh:         make(chan struct{}),
		closedCh:       make(chan struct{}),
	}, nil
}

func (ts *testSetup) waitUntilServerIsUp() error {
	serverIsUp := func() bool {
		resp, err := ts.health()
		return err == nil && resp.Bootstrapped
	}
	if waitUntil(serverIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	serverIsDown := func() bool {
		_, err := ts.health()
		return err != nil
	}
	if waitUntil(serverIsDown, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServer() error {
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
	ts.db, err = cluster.NewDatabase(ts.namespaces,
		ts.hostID, ts.topoInit, ts.storageOpts)
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

	return <-resultCh
}

func (ts *testSetup) stopServer() error {
	ts.doneCh <- struct{}{}

	if err := ts.waitUntilServerIsDown(); err != nil {
		return err
	}

	// Wait for graceful server close
	<-ts.closedCh
	return nil
}

func (ts *testSetup) writeBatch(namespace ts.ID, seriesList seriesList) error {
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
