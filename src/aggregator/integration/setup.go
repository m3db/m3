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
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler"
	httpserver "github.com/m3db/m3aggregator/server/http"
	msgpackserver "github.com/m3db/m3aggregator/server/msgpack"
	"github.com/m3db/m3aggregator/services/m3aggregator/serve"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/require"
)

// nolint: megacheck, varcheck, deadcode
var (
	msgpackAddrArg                = flag.String("msgpackAddr", "0.0.0.0:6000", "msgpack server address")
	httpAddrArg                   = flag.String("httpAddr", "0.0.0.0:6001", "http server address")
	errServerStartTimedOut        = errors.New("server took too long to start")
	errServerStopTimedOut         = errors.New("server took too long to stop")
	errElectionStateChangeTimeout = errors.New("server took too long to change election state")
)

// nowSetterFn is the function that sets the current time.
// nolint: megacheck
type nowSetterFn func(t time.Time)

// nolint: megacheck
type testSetup struct {
	opts              testOptions
	msgpackAddr       string
	httpAddr          string
	msgpackServerOpts msgpackserver.Options
	httpServerOpts    httpserver.Options
	aggregator        aggregator.Aggregator
	aggregatorOpts    aggregator.Options
	handler           aggregator.Handler
	electionKey       string
	leaderValue       string
	leaderService     services.LeaderService
	electionCluster   *testCluster
	getNowFn          clock.NowFn
	setNowFn          nowSetterFn
	workerPool        xsync.WorkerPool
	results           *[]aggregated.MetricWithStoragePolicy
	resultLock        *sync.Mutex

	// Signals.
	doneCh   chan struct{}
	closedCh chan struct{}
}

// nolint: megacheck, deadcode
func newTestSetup(t *testing.T, opts testOptions) *testSetup {
	if opts == nil {
		opts = newTestOptions()
	}

	// Set up the msgpack server address.
	msgpackAddr := *msgpackAddrArg
	if addr := opts.MsgpackAddr(); addr != "" {
		msgpackAddr = addr
	}

	// Set up the http server address.
	httpAddr := *httpAddrArg
	if addr := opts.HTTPAddr(); addr != "" {
		httpAddr = addr
	}

	// Set up worker pool.
	workerPool := xsync.NewWorkerPool(opts.WorkerPoolSize())
	workerPool.Init()

	// Set up getter and setter for now.
	var lock sync.RWMutex
	now := time.Now().Truncate(time.Hour)
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

	// Create the server options.
	msgpackServerOpts := msgpackserver.NewOptions()
	httpServerOpts := httpserver.NewOptions()

	// Creating the aggregator options.
	aggregatorOpts := aggregator.NewOptions()
	clockOpts := aggregatorOpts.ClockOptions()
	aggregatorOpts = aggregatorOpts.SetClockOptions(clockOpts.SetNowFn(getNowFn))
	entryPool := aggregator.NewEntryPool(nil)
	entryPool.Init(func() *aggregator.Entry {
		return aggregator.NewEntry(nil, aggregatorOpts)
	})
	aggregatorOpts = aggregatorOpts.SetEntryPool(entryPool)

	// Set up election manager.
	leaderValue := opts.InstanceID()
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	electionKey := fmt.Sprintf(opts.ElectionKeyFmt(), opts.ShardSetID())
	electionCluster := newTestCluster(t)
	leaderService := electionCluster.LeaderService()
	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetElectionKeyFmt(opts.ElectionKeyFmt()).
		SetLeaderService(leaderService)
	electionManager := aggregator.NewElectionManager(electionManagerOpts)
	aggregatorOpts = aggregatorOpts.SetElectionManager(electionManager)

	// Set up placement watcher.
	shardSet := make([]shard.Shard, opts.NumShards())
	for i := 0; i < opts.NumShards(); i++ {
		shardSet[i] = shard.NewShard(uint32(i)).
			SetState(shard.Initializing).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
	}
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(opts.InstanceID()).
		SetShards(shards).
		SetShardSetID(opts.ShardSetID())
	testPlacement := placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shards.AllIDs())
	stagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]placement.Placement{testPlacement})
	stagedPlacementProto, err := stagedPlacement.Proto()
	require.NoError(t, err)
	placementKey := opts.PlacementKVKey()
	placementStore := opts.KVStore()
	_, err = placementStore.SetIfNotExists(placementKey, stagedPlacementProto)
	require.NoError(t, err)
	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKey).
		SetStagedPlacementStore(placementStore)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	require.NoError(t, placementWatcher.Watch())
	aggregatorOpts = aggregatorOpts.
		SetInstanceID(opts.InstanceID()).
		SetStagedPlacementWatcher(placementWatcher)

	// Set up flush manager.
	flushManagerOpts := aggregator.NewFlushManagerOptions().
		SetElectionManager(electionManager).
		SetFlushTimesKeyFmt(opts.FlushTimesKeyFmt()).
		SetFlushTimesStore(opts.KVStore()).
		SetJitterEnabled(opts.JitterEnabled()).
		SetMaxJitterFn(opts.MaxJitterFn()).
		SetInstanceID(opts.InstanceID()).
		SetStagedPlacementWatcher(placementWatcher)
	flushManager := aggregator.NewFlushManager(flushManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushManager(flushManager)

	// Set up the handler.
	var (
		results    []aggregated.MetricWithStoragePolicy
		resultLock sync.Mutex
	)
	handleFn := func(metric aggregated.Metric, sp policy.StoragePolicy) error {
		resultLock.Lock()
		results = append(results, aggregated.MetricWithStoragePolicy{
			Metric:        metric,
			StoragePolicy: sp,
		})
		resultLock.Unlock()
		return nil
	}
	handler := handler.NewDecodingHandler(handleFn)
	aggregatorOpts = aggregatorOpts.SetFlushHandler(handler)

	return &testSetup{
		opts:              opts,
		msgpackAddr:       msgpackAddr,
		httpAddr:          httpAddr,
		msgpackServerOpts: msgpackServerOpts,
		httpServerOpts:    httpServerOpts,
		aggregatorOpts:    aggregatorOpts,
		handler:           handler,
		electionKey:       electionKey,
		leaderValue:       leaderValue,
		leaderService:     leaderService,
		electionCluster:   electionCluster,
		getNowFn:          getNowFn,
		setNowFn:          setNowFn,
		workerPool:        workerPool,
		results:           &results,
		resultLock:        &resultLock,
		doneCh:            make(chan struct{}),
		closedCh:          make(chan struct{}),
	}
}

func (ts *testSetup) newClient() *client {
	return newClient(ts.msgpackAddr, ts.opts.ClientBatchSize(), ts.opts.ClientConnectTimeout())
}

func (ts *testSetup) waitUntilServerIsUp() error {
	c := ts.newClient()
	defer c.close()

	serverIsUp := func() bool { return c.testConnection() }
	if waitUntil(serverIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) startServer() error {
	errCh := make(chan error, 1)

	// Creating the aggregator.
	ts.aggregator = aggregator.NewAggregator(ts.aggregatorOpts)
	if err := ts.aggregator.Open(); err != nil {
		return err
	}

	go func() {
		if err := serve.Serve(
			ts.msgpackAddr,
			ts.msgpackServerOpts,
			ts.httpAddr,
			ts.httpServerOpts,
			ts.aggregator,
			ts.doneCh,
		); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
		close(ts.closedCh)
	}()

	go func() {
		select {
		case errCh <- ts.waitUntilServerIsUp():
		default:
		}
	}()

	return <-errCh
}

func (ts *testSetup) waitUntilLeader() error {
	isLeader := func() bool {
		leader, err := ts.leaderService.Leader(ts.electionKey)
		if err != nil {
			return false
		}
		return leader == ts.leaderValue
	}
	waitUntil(isLeader, ts.opts.ElectionStateChangeTimeout())
	return nil
}

func (ts *testSetup) sortedResults() []aggregated.MetricWithStoragePolicy {
	sort.Sort(byTimeIDPolicyAscending(*ts.results))
	return *ts.results
}

func (ts *testSetup) stopServer() error {
	if err := ts.aggregator.Close(); err != nil {
		return err
	}
	close(ts.doneCh)

	// Wait for graceful server shutdown.
	<-ts.closedCh
	return nil
}

func (ts *testSetup) close() {
	ts.electionCluster.Close()
}
