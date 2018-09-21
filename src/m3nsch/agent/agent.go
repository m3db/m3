// Copyright (c) 2017 Uber Technologies, Inc.
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

package agent

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/m3nsch"
	"github.com/m3db/m3/src/m3nsch/datums"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

var (
	errCannotStartNotInitialized = fmt.Errorf("unable to start, agent is not initialized")
	errCannotStopNotInitialized  = fmt.Errorf("unable to stop, agent is not initialized")
	errAlreadyInitialized        = fmt.Errorf("unable to initialize, agent already initialized")
)

type m3nschAgent struct {
	sync.RWMutex
	token       string              // workload token
	workload    m3nsch.Workload     // workload to operate upon
	registry    datums.Registry     // workload fake metric registry
	session     client.Session      // m3db session to operate upon
	agentStatus m3nsch.Status       // agent status
	opts        m3nsch.AgentOptions // agent options
	logger      xlog.Logger         // logger
	metrics     agentMetrics        // agent performance metrics
	workerChans workerChannels      // worker-idx -> channel for worker notification
	workerWg    sync.WaitGroup      // used to track when workers are finished
	params      workerParams        // worker params
}

type workerParams struct {
	sync.RWMutex
	fn         workerFn          // workerFn (read|write)
	workingSet []generatedMetric // metrics corresponding to workload
	ranges     []workerRange     // worker-idx -> workingSet idx range
}

// New returns a new Agent.
func New(
	registry datums.Registry,
	opts m3nsch.AgentOptions,
) m3nsch.Agent {
	ms := &m3nschAgent{
		registry: registry,
		opts:     opts,
		logger:   opts.InstrumentOptions().Logger(),
		params: workerParams{
			fn: workerWriteFn,
		},
	}
	ms.metrics = agentMetrics{
		writeMethodMetrics: ms.newMethodMetrics("write"),
	}
	return ms

}

func newWorkerChannels(numWorkers int) workerChannels {
	var chans workerChannels
	for i := 0; i < numWorkers; i++ {
		chans = append(chans, make(chan workerNotification))
	}
	return chans
}

func (ms *m3nschAgent) closeWorkerChannelsWithLock() {
	ms.workerChans.close()
	ms.workerChans = nil
}

func (ms *m3nschAgent) notifyWorkersWithLock(msg workerNotification) {
	ms.workerChans.notify(msg)
}

func (ms *m3nschAgent) resetWithLock() {
	if ms.workerChans != nil {
		ms.closeWorkerChannelsWithLock()
	}
	if ms.session != nil {
		ms.session.Close()
		ms.session = nil
	}
	ms.workload = m3nsch.Workload{}
	ms.agentStatus = m3nsch.StatusUninitialized
	ms.params.workingSet = nil
	ms.params.ranges = nil
}

func (ms *m3nschAgent) setWorkerParams(workload m3nsch.Workload) {
	ms.params.Lock()
	defer ms.params.Unlock()

	var (
		current     = ms.params.workingSet
		cardinality = workload.Cardinality
	)

	if len(current) > cardinality {
		current = current[:cardinality]
	}
	for i := len(current); i < cardinality; i++ {
		idx := workload.MetricStartIdx + i
		current = append(current, generatedMetric{
			name:       fmt.Sprintf("%v.m%d", workload.MetricPrefix, idx),
			timeseries: ms.registry.Get(i),
		})
	}
	ms.params.workingSet = current

	concurrency := ms.opts.Concurrency()
	numMetricsPerWorker := len(current) / concurrency
	workerRanges := make([]workerRange, concurrency)
	for i := 0; i < concurrency; i++ {
		workerRanges[i] = workerRange{
			startIdx: i * numMetricsPerWorker,
			endIdx:   (i+1)*numMetricsPerWorker - 1,
			lastIdx:  -1,
		}
	}
	ms.params.ranges = workerRanges
}

func (ms *m3nschAgent) Status() m3nsch.AgentStatus {
	ms.RLock()
	defer ms.RUnlock()
	return m3nsch.AgentStatus{
		Status: ms.agentStatus,
		Token:  ms.token,
	}
}

func (ms *m3nschAgent) Workload() m3nsch.Workload {
	ms.RLock()
	defer ms.RUnlock()
	return ms.workload
}

func (ms *m3nschAgent) SetWorkload(w m3nsch.Workload) {
	ms.Lock()
	defer ms.Unlock()
	ms.workload = w
	ms.setWorkerParams(w)

	if ms.agentStatus == m3nsch.StatusRunning {
		ms.notifyWorkersWithLock(workerNotification{update: true})
	}
}

func (ms *m3nschAgent) Init(
	token string,
	w m3nsch.Workload,
	force bool,
	targetZone string,
	targetEnv string,
) error {
	ms.Lock()
	defer ms.Unlock()
	status := ms.agentStatus
	if status != m3nsch.StatusUninitialized && !force {
		return errAlreadyInitialized
	}

	if status == m3nsch.StatusRunning {
		if err := ms.stopWithLock(); err != nil {
			return err
		}
	}

	session, err := ms.opts.NewSessionFn()(targetZone, targetEnv)
	if err != nil {
		return err
	}
	ms.session = session
	ms.token = token
	ms.workload = w
	ms.setWorkerParams(w)
	ms.agentStatus = m3nsch.StatusInitialized
	return nil
}

func (ms *m3nschAgent) Start() error {
	ms.Lock()
	defer ms.Unlock()
	if ms.agentStatus != m3nsch.StatusInitialized {
		return errCannotStartNotInitialized
	}
	concurrency := ms.opts.Concurrency()
	ms.workerChans = newWorkerChannels(concurrency)
	ms.agentStatus = m3nsch.StatusRunning
	ms.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go ms.runWorker(i, ms.workerChans[i])
	}
	return nil
}

func (ms *m3nschAgent) stopWithLock() error {
	status := ms.agentStatus
	if status == m3nsch.StatusUninitialized {
		return errCannotStopNotInitialized
	}

	if status == m3nsch.StatusRunning {
		ms.notifyWorkersWithLock(workerNotification{stop: true})
		ms.workerWg.Wait()
	}

	ms.resetWithLock()
	return nil
}

func (ms *m3nschAgent) Stop() error {
	ms.Lock()
	defer ms.Unlock()
	return ms.stopWithLock()
}

func (ms *m3nschAgent) MaxQPS() int64 {
	return int64(ms.opts.Concurrency()) * ms.opts.MaxWorkerQPS()
}

// nolint: unparam
func (ms *m3nschAgent) newMethodMetrics(method string) instrument.MethodMetrics {
	subScope := ms.opts.InstrumentOptions().MetricsScope().SubScope("agent")
	return instrument.NewMethodMetrics(subScope, method, ms.opts.InstrumentOptions().MetricsSamplingRate())
}

func (ms *m3nschAgent) tickPeriodWithLock() time.Duration {
	var (
		qps          = ms.workload.IngressQPS
		numWorkers   = ms.opts.Concurrency()
		qpsPerWorker = float64(qps) / float64(numWorkers)
		tickPeriod   = time.Duration(1000*1000*1000/qpsPerWorker) * time.Nanosecond
	)
	return tickPeriod
}

func (ms *m3nschAgent) workerParams() (xtime.Unit, string, time.Time, time.Duration) {
	ms.params.RLock()
	defer ms.params.RUnlock()
	return ms.opts.TimeUnit(), ms.workload.Namespace, ms.workload.BaseTime, ms.tickPeriodWithLock()
}

func (ms *m3nschAgent) nextWorkerMetric(workerIdx int) generatedMetric {
	ms.params.RLock()
	defer ms.params.RUnlock()
	metricIdx := ms.params.ranges[workerIdx].next()
	return ms.params.workingSet[metricIdx]
}

func (ms *m3nschAgent) runWorker(workerIdx int, workerCh chan workerNotification) {
	defer ms.workerWg.Done()
	var (
		methodMetrics                            = ms.metrics.writeMethodMetrics
		timeUnit, namespace, fakeNow, tickPeriod = ms.workerParams()
		tickLoop                                 = time.NewTicker(tickPeriod)
	)
	defer tickLoop.Stop()
	for {
		select {
		case msg := <-workerCh:
			if msg.stop {
				return
			}
			if msg.update {
				tickLoop.Stop()
				timeUnit, namespace, fakeNow, tickPeriod = ms.workerParams()
				tickLoop = time.NewTicker(tickPeriod)
			}

		case <-tickLoop.C:
			fakeNow = fakeNow.Add(tickPeriod)
			metric := ms.nextWorkerMetric(workerIdx)
			start := time.Now()
			err := ms.params.fn(workerIdx, ms.session, namespace, metric, fakeNow, timeUnit)
			elapsed := time.Since(start)
			methodMetrics.ReportSuccessOrError(err, elapsed)
		}
	}
}

type generatedMetric struct {
	name       string
	timeseries datums.SyntheticTimeSeries
}

type workerRange struct {
	startIdx int // inclusive
	endIdx   int // exclusive
	lastIdx  int // last idx returned
}

func (wr *workerRange) next() int {
	i := wr.lastIdx
	next := wr.startIdx + ((i + 1) % (wr.endIdx - wr.startIdx + 1))
	wr.lastIdx = next
	return next
}

type workerNotification struct {
	stop   bool
	update bool
}

type workerChannels []chan workerNotification

func (c workerChannels) close() {
	for _, ch := range c {
		close(ch)
	}
}

func (c workerChannels) notify(msg workerNotification) {
	for _, ch := range c {
		ch <- msg
	}
}

type agentMetrics struct {
	writeMethodMetrics instrument.MethodMetrics
}

type workerFn func(workerIdx int, session client.Session, namespace string, metric generatedMetric, t time.Time, u xtime.Unit) error

func workerWriteFn(_ int, session client.Session, namespace string, metric generatedMetric, t time.Time, u xtime.Unit) error {
	return session.Write(ident.StringID(namespace), ident.StringID(metric.name), t, metric.timeseries.Next(), u, nil)
}
