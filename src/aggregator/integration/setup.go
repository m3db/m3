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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/runtime"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	m3msgserver "github.com/m3db/m3/src/aggregator/server/m3msg"
	rawtcpserver "github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	errServerStartTimedOut   = errors.New("server took too long to start")
	errLeaderElectionTimeout = errors.New("took too long to become leader")
)

type testServerSetup struct {
	opts             testServerOptions
	m3msgAddr        string
	rawTCPAddr       string
	httpAddr         string
	m3msgServerOpts  m3msgserver.Options
	rawTCPServerOpts rawtcpserver.Options
	httpServerOpts   httpserver.Options
	aggregator       aggregator.Aggregator
	aggregatorOpts   aggregator.Options
	handler          handler.Handler
	electionKey      string
	leaderValue      string
	leaderService    services.LeaderService
	electionCluster  *testCluster
	workerPool       xsync.WorkerPool
	results          *[]aggregated.MetricWithStoragePolicy
	resultLock       *sync.Mutex

	// Signals.
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestServerSetup(t *testing.T, opts testServerOptions) *testServerSetup {
	if opts == nil {
		opts = newTestServerOptions()
	}

	// TODO: based on environment variable, use M3MSG aggregator as default
	// server and client, run both legacy and M3MSG tests by setting it to
	// different type in the Makefile.

	// Set up worker pool.
	workerPool := xsync.NewWorkerPool(opts.WorkerPoolSize())
	workerPool.Init()

	// Create the server options.
	rwOpts := xio.NewOptions()
	rawTCPServerOpts := rawtcpserver.NewOptions().SetRWOptions(rwOpts)
	m3msgServerOpts := m3msgserver.NewOptions()
	httpServerOpts := httpserver.NewOptions().
		// use a new mux per test to avoid collisions registering the same handlers between tests.
		SetMux(http.NewServeMux())

	// Creating the aggregator options.
	clockOpts := opts.ClockOptions()
	aggregatorOpts := aggregator.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetAggregationTypesOptions(opts.AggregationTypesOptions()).
		SetEntryCheckInterval(opts.EntryCheckInterval()).
		SetMaxAllowedForwardingDelayFn(opts.MaxAllowedForwardingDelayFn()).
		SetDiscardNaNAggregatedValues(opts.DiscardNaNAggregatedValues())

	// Set up placement manager.
	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetClockOptions(clockOpts).
		SetStagedPlacementKey(opts.PlacementKVKey()).
		SetStagedPlacementStore(opts.KVStore())
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetClockOptions(clockOpts).
		SetInstanceID(opts.InstanceID()).
		SetStagedPlacementWatcher(placementWatcher)
	placementManager := aggregator.NewPlacementManager(placementManagerOpts)
	aggregatorOpts = aggregatorOpts.
		SetShardFn(opts.ShardFn()).
		SetPlacementManager(placementManager)

	// Set up flush times manager.
	flushTimesManagerOpts := aggregator.NewFlushTimesManagerOptions().
		SetClockOptions(clockOpts).
		SetFlushTimesKeyFmt(opts.FlushTimesKeyFmt()).
		SetFlushTimesStore(opts.KVStore())
	flushTimesManager := aggregator.NewFlushTimesManager(flushTimesManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushTimesManager(flushTimesManager)

	// Set up election manager.
	leaderValue := opts.InstanceID()
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	electionKey := fmt.Sprintf(opts.ElectionKeyFmt(), opts.ShardSetID())
	electionCluster := opts.ElectionCluster()
	if electionCluster == nil {
		electionCluster = newTestCluster(t)
	}
	leaderService := electionCluster.LeaderService()
	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetClockOptions(clockOpts).
		SetCampaignOptions(campaignOpts).
		SetElectionKeyFmt(opts.ElectionKeyFmt()).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager)
	electionManager := aggregator.NewElectionManager(electionManagerOpts)
	aggregatorOpts = aggregatorOpts.SetElectionManager(electionManager)

	// Set up flush manager.
	flushManagerOpts := aggregator.NewFlushManagerOptions().
		SetClockOptions(clockOpts).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetJitterEnabled(opts.JitterEnabled()).
		SetMaxJitterFn(opts.MaxJitterFn())
	flushManager := aggregator.NewFlushManager(flushManagerOpts)
	aggregatorOpts = aggregatorOpts.SetFlushManager(flushManager)

	// Set up admin client.
	clientOpts := aggclient.NewOptions().
		SetClockOptions(clockOpts).
		SetConnectionOptions(opts.ClientConnectionOptions()).
		SetShardFn(opts.ShardFn()).
		SetStagedPlacementWatcherOptions(placementWatcherOpts).
		SetRWOptions(rwOpts)
	c, err := aggclient.NewClient(clientOpts)
	require.NoError(t, err)
	adminClient, ok := c.(aggclient.AdminClient)
	require.True(t, ok)
	require.NoError(t, adminClient.Init())
	aggregatorOpts = aggregatorOpts.SetAdminClient(adminClient)

	// Set up the handler.
	var (
		results    []aggregated.MetricWithStoragePolicy
		resultLock sync.Mutex
	)
	handler := &capturingHandler{results: &results, resultLock: &resultLock}
	pw, err := handler.NewWriter(tally.NoopScope)
	if err != nil {
		panic(err.Error())
	}
	aggregatorOpts = aggregatorOpts.SetFlushHandler(handler).SetPassthroughWriter(pw)

	// Set up entry pool.
	runtimeOpts := runtime.NewOptions()
	entryPool := aggregator.NewEntryPool(nil)
	entryPool.Init(func() *aggregator.Entry {
		return aggregator.NewEntry(nil, runtimeOpts, aggregatorOpts)
	})
	aggregatorOpts = aggregatorOpts.SetEntryPool(entryPool)

	// Set up elem pools.
	counterElemPool := aggregator.NewCounterElemPool(nil)
	aggregatorOpts = aggregatorOpts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.MustNewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, aggregatorOpts)
	})

	timerElemPool := aggregator.NewTimerElemPool(nil)
	aggregatorOpts = aggregatorOpts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.MustNewTimerElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, aggregatorOpts)
	})

	gaugeElemPool := aggregator.NewGaugeElemPool(nil)
	aggregatorOpts = aggregatorOpts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.MustNewGaugeElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, aggregator.NoPrefixNoSuffix, aggregatorOpts)
	})

	return &testServerSetup{
		opts:             opts,
		rawTCPAddr:       opts.RawTCPAddr(),
		httpAddr:         opts.HTTPAddr(),
		rawTCPServerOpts: rawTCPServerOpts,
		m3msgServerOpts:  m3msgServerOpts,
		httpServerOpts:   httpServerOpts,
		aggregatorOpts:   aggregatorOpts,
		handler:          handler,
		electionKey:      electionKey,
		leaderValue:      leaderValue,
		leaderService:    leaderService,
		electionCluster:  electionCluster,
		workerPool:       workerPool,
		results:          &results,
		resultLock:       &resultLock,
		doneCh:           make(chan struct{}),
		closedCh:         make(chan struct{}),
	}
}

func (ts *testServerSetup) newClient() *client {
	connectTimeout := ts.opts.ClientConnectionOptions().ConnectionTimeout()
	return newClient(ts.rawTCPAddr, ts.opts.ClientBatchSize(), connectTimeout)
}

func (ts *testServerSetup) getStatusResponse(path string, response interface{}) error {
	resp, err := http.Get("http://" + ts.httpAddr + path) //nolint
	if err != nil {
		return err
	}

	defer resp.Body.Close() //nolint:errcheck
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got a non-200 status code: %v", resp.StatusCode)
	}
	return json.Unmarshal(b, response)
}

func (ts *testServerSetup) waitUntilServerIsUp() error {
	isUp := func() bool {
		var resp httpserver.Response
		if err := ts.getStatusResponse(httpserver.HealthPath, &resp); err != nil {
			return false
		}

		if resp.State == "OK" {
			return true
		}

		return false
	}

	if waitUntil(isUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}

	return errServerStartTimedOut
}

func (ts *testServerSetup) startServer() error {
	errCh := make(chan error, 1)

	// Creating the aggregator.
	ts.aggregator = aggregator.NewAggregator(ts.aggregatorOpts)
	if err := ts.aggregator.Open(); err != nil {
		return err
	}

	instrumentOpts := instrument.NewOptions()
	serverOpts := serve.NewOptions(instrumentOpts).
		SetM3MsgAddr(ts.m3msgAddr).
		SetM3MsgServerOpts(ts.m3msgServerOpts).
		SetRawTCPAddr(ts.rawTCPAddr).
		SetRawTCPServerOpts(ts.rawTCPServerOpts).
		SetHTTPAddr(ts.httpAddr).
		SetHTTPServerOpts(ts.httpServerOpts).
		SetRWOptions(xio.NewOptions())

	go func() {
		if err := serve.Serve(
			ts.aggregator,
			ts.doneCh,
			serverOpts,
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

func (ts *testServerSetup) waitUntilLeader() error {
	isLeader := func() bool {
		var resp httpserver.StatusResponse
		if err := ts.getStatusResponse(httpserver.StatusPath, &resp); err != nil {
			return false
		}

		if resp.Status.FlushStatus.ElectionState == aggregator.LeaderState {
			return true
		}
		return false
	}

	if waitUntil(isLeader, ts.opts.ElectionStateChangeTimeout()) {
		return nil
	}

	return errLeaderElectionTimeout
}

func (ts *testServerSetup) sortedResults() []aggregated.MetricWithStoragePolicy {
	sort.Sort(byTimeIDPolicyAscending(*ts.results))
	return *ts.results
}

func (ts *testServerSetup) stopServer() error {
	if err := ts.aggregator.Close(); err != nil {
		return err
	}
	close(ts.doneCh)

	// Wait for graceful server shutdown.
	<-ts.closedCh
	return nil
}

func (ts *testServerSetup) close() {
	ts.electionCluster.Close()
}

type capturingWriter struct {
	results    *[]aggregated.MetricWithStoragePolicy
	resultLock *sync.Mutex
}

func (w *capturingWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	w.resultLock.Lock()
	var fullID []byte
	fullID = append(fullID, mp.ChunkedID.Prefix...)
	fullID = append(fullID, mp.ChunkedID.Data...)
	fullID = append(fullID, mp.ChunkedID.Suffix...)
	metric := aggregated.Metric{
		ID:        fullID,
		TimeNanos: mp.TimeNanos,
		Value:     mp.Value,
	}
	*w.results = append(*w.results, aggregated.MetricWithStoragePolicy{
		Metric:        metric,
		StoragePolicy: mp.StoragePolicy,
	})
	w.resultLock.Unlock()
	return nil
}

func (w *capturingWriter) Flush() error { return nil }
func (w *capturingWriter) Close() error { return nil }

type capturingHandler struct {
	results    *[]aggregated.MetricWithStoragePolicy
	resultLock *sync.Mutex
}

func (h *capturingHandler) NewWriter(tally.Scope) (writer.Writer, error) {
	return &capturingWriter{results: h.results, resultLock: h.resultLock}, nil
}

func (h *capturingHandler) Close() {}
