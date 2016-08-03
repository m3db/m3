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
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/storage"

	"github.com/uber/tchannel-go"
)

var (
	httpClusterAddr     = flag.String("clusterhttpaddr", "0.0.0.0:9000", "Cluster HTTP server address")
	tchannelClusterAddr = flag.String("clustertchanneladdr", "0.0.0.0:9001", "Cluster TChannel server address")
	httpNodeAddr        = flag.String("nodehttpaddr", "0.0.0.0:9002", "Node HTTP server address")
	tchannelNodeAddr    = flag.String("nodetchanneladdr", "0.0.0.0:9003", "Node TChannel server address")

	errServerStartTimedOut = errors.New("server took too long to start")
	errServerStopTimedOut  = errors.New("server took too long to stop")
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

type testSetup struct {
	opts           testOptions
	clientOpts     m3db.ClientOptions
	dbOpts         m3db.DatabaseOptions
	shardingScheme m3db.ShardScheme
	getNowFn       m3db.NowFn
	setNowFn       nowSetterFn
	tchannelClient rpc.TChanNode
	m3dbClient     m3db.Client
	workerPool     m3db.WorkerPool

	// things that need to be cleaned up
	channel        *tchannel.Channel
	filePathPrefix string
}

func newTestSetup(opts testOptions) (*testSetup, error) {
	if opts == nil {
		opts = newTestOptions()
	}

	var dbOpts m3db.DatabaseOptions
	dbOpts = storage.NewDatabaseOptions().NewBootstrapFn(func() m3db.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(dbOpts)
	})

	// Set up sharding scheme
	shardingScheme, err := server.DefaultShardingScheme()
	if err != nil {
		return nil, err
	}

	clientOpts, err := server.DefaultClientOptions(*tchannelNodeAddr, shardingScheme)
	if err != nil {
		return nil, err
	}
	clientOpts = clientOpts.ClusterConnectTimeout(opts.GetClusterConnectionTimeout())

	// Set up tchannel client
	channel, tc, err := tchannelClient(*tchannelNodeAddr)
	if err != nil {
		return nil, err
	}

	// Set up m3db client
	mc, err := m3dbClient(clientOpts)
	if err != nil {
		return nil, err
	}

	// Set up worker pool
	workerPool := pool.NewWorkerPool(opts.GetWorkerPoolSize())
	workerPool.Init()

	// Set up getter and setter for now
	var lock sync.RWMutex
	now := time.Now().Truncate(dbOpts.GetBlockSize())
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
	dbOpts = dbOpts.NowFn(getNowFn)

	// Set up file path prefix
	filePathPrefix, err := ioutil.TempDir("", "integration-test")
	if err != nil {
		return nil, err
	}
	dbOpts = dbOpts.FilePathPrefix(filePathPrefix)

	// Set up persistence manager
	dbOpts = dbOpts.NewPersistenceManagerFn(func(opts m3db.DatabaseOptions) m3db.PersistenceManager {
		return fs.NewPersistenceManager(opts)
	})

	return &testSetup{
		opts:           opts,
		clientOpts:     clientOpts,
		dbOpts:         dbOpts,
		shardingScheme: shardingScheme,
		getNowFn:       getNowFn,
		setNowFn:       setNowFn,
		tchannelClient: tc,
		m3dbClient:     mc,
		workerPool:     workerPool,
		channel:        channel,
		filePathPrefix: filePathPrefix,
	}, nil
}

func (ts *testSetup) waitUntilServerIsUp() error {
	fakeRequest := rpc.NewFetchRequest()
	serverIsUp := func() bool { _, err := ts.fetch(fakeRequest); return err == nil }
	if waitUntil(serverIsUp, ts.opts.GetServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	fakeRequest := rpc.NewFetchRequest()
	serverIsDown := func() bool { _, err := ts.fetch(fakeRequest); return err != nil }
	if waitUntil(serverIsDown, ts.opts.GetServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServer(doneCh chan struct{}) error {
	go server.Serve(
		*httpClusterAddr,
		*tchannelClusterAddr,
		*httpNodeAddr,
		*tchannelNodeAddr,
		ts.clientOpts,
		ts.dbOpts,
		doneCh,
	)

	return ts.waitUntilServerIsUp()
}

func (ts *testSetup) stopServer(doneCh chan<- struct{}) error {
	doneCh <- struct{}{}

	return ts.waitUntilServerIsDown()
}

func (ts *testSetup) writeBatch(dm dataMap) error {
	if ts.opts.GetUseTChannelClientForWriting() {
		return tchannelClientWriteBatch(ts.tchannelClient, ts.opts.GetWriteRequestTimeout(), dm)
	}
	return m3dbClientWriteBatch(ts.m3dbClient, ts.workerPool, dm)
}

func (ts *testSetup) fetch(req *rpc.FetchRequest) ([]m3db.Datapoint, error) {
	if ts.opts.GetUseTChannelClientForReading() {
		return tchannelClientFetch(ts.tchannelClient, ts.opts.GetReadRequestTimeout(), req)
	}
	return m3dbClientFetch(ts.m3dbClient, req)
}

func (ts *testSetup) close() {
	if ts.channel != nil {
		ts.channel.Close()
	}
	if ts.filePathPrefix != "" {
		os.RemoveAll(ts.filePathPrefix)
	}
}
