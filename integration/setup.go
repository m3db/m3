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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"

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
	testNamespaces         = []string{"testNs1", "testNs2"}
)

// nowSetterFn is the function that sets the current time
type nowSetterFn func(t time.Time)

type testSetup struct {
	opts           testOptions
	clientOpts     client.Options
	storageOpts    storage.Options
	shardSet       sharding.ShardSet
	getNowFn       clock.NowFn
	setNowFn       nowSetterFn
	tchannelClient rpc.TChanNode
	m3dbClient     client.Client
	workerPool     pool.WorkerPool

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
		id = "testhost"
	}

	clientOpts, err := server.DefaultClientOptions(id, *tchannelNodeAddr, shardSet)
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
	now := time.Now().Truncate(storageOpts.GetRetentionOptions().GetBlockSize())
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
	storageOpts = storageOpts.ClockOptions(storageOpts.GetClockOptions().NowFn(getNowFn))

	// Set up file path prefix
	filePathPrefix, err := ioutil.TempDir("", "integration-test")
	if err != nil {
		return nil, err
	}

	var namespaces []namespace.Metadata
	for _, ns := range testNamespaces {
		namespaces = append(namespaces, namespace.NewMetadata(ns, namespace.NewOptions()))
	}

	fsOpts := fs.NewOptions().FilePathPrefix(filePathPrefix)

	storageOpts = storageOpts.CommitLogOptions(storageOpts.GetCommitLogOptions().FilesystemOptions(fsOpts))

	// Set up persistence manager
	storageOpts = storageOpts.NewPersistManagerFn(func() persist.Manager {
		return fs.NewPersistManager(fsOpts)
	})

	return &testSetup{
		opts:           opts,
		clientOpts:     clientOpts,
		storageOpts:    storageOpts,
		shardSet:       shardSet,
		getNowFn:       getNowFn,
		setNowFn:       setNowFn,
		tchannelClient: tc,
		m3dbClient:     mc,
		workerPool:     workerPool,
		channel:        channel,
		filePathPrefix: filePathPrefix,
		namespaces:     namespaces,
		doneCh:         make(chan struct{}),
		closedCh:       make(chan struct{}),
	}, nil
}

func (ts *testSetup) fakeRequest() *rpc.FetchRequest {
	req := rpc.NewFetchRequest()
	req.NameSpace = testNamespaces[0]
	return req
}

func (ts *testSetup) waitUntilServerIsUp() error {
	fakeRequest := ts.fakeRequest()
	serverIsUp := func() bool { _, err := ts.fetch(fakeRequest); return err == nil }
	if waitUntil(serverIsUp, ts.opts.GetServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	fakeRequest := ts.fakeRequest()
	serverIsDown := func() bool { _, err := ts.fetch(fakeRequest); return err != nil }
	if waitUntil(serverIsDown, ts.opts.GetServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServer() error {
	resultCh := make(chan error, 1)

	go func() {
		err := server.Serve(
			*httpClusterAddr,
			*tchannelClusterAddr,
			*httpNodeAddr,
			*tchannelNodeAddr,
			ts.namespaces,
			ts.clientOpts,
			ts.storageOpts,
			ts.doneCh)
		if err != nil {
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

func (ts *testSetup) writeBatch(namespace string, seriesList seriesList) error {
	if ts.opts.GetUseTChannelClientForWriting() {
		return tchannelClientWriteBatch(ts.tchannelClient, ts.opts.GetWriteRequestTimeout(), namespace, seriesList)
	}
	return m3dbClientWriteBatch(ts.m3dbClient, ts.workerPool, namespace, seriesList)
}

func (ts *testSetup) fetch(req *rpc.FetchRequest) ([]ts.Datapoint, error) {
	if ts.opts.GetUseTChannelClientForReading() {
		return tchannelClientFetch(ts.tchannelClient, ts.opts.GetReadRequestTimeout(), req)
	}
	return m3dbClientFetch(ts.m3dbClient, req)
}

func (ts *testSetup) truncate(req *rpc.TruncateRequest) (int64, error) {
	if ts.opts.GetUseTChannelClientForTruncation() {
		return tchannelClientTruncate(ts.tchannelClient, ts.opts.GetTruncateRequestTimeout(), req)
	}
	return m3dbClientTruncate(ts.m3dbClient, req)
}

func (ts *testSetup) close() {
	if ts.channel != nil {
		ts.channel.Close()
	}
	if ts.filePathPrefix != "" {
		os.RemoveAll(ts.filePathPrefix)
	}
}
