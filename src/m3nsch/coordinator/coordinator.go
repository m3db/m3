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

package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/src/m3nsch"
	"github.com/m3db/m3db/src/m3nsch/rpc"
	"github.com/m3db/m3db/src/m3nsch/rpc/convert"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"google.golang.org/grpc"
)

var (
	errNoEndpointsProvided = fmt.Errorf("no endpoints provided")
)

type m3nschCoordinator struct {
	sync.Mutex
	opts        m3nsch.CoordinatorOptions
	clients     map[string]*m3nschClient
	newClientFn newM3nschClientFn
}

// New returns a new Coordinator process with provided endpoints treated
// as Agent processes.
func New(
	opts m3nsch.CoordinatorOptions,
	endpoints []string,
) (m3nsch.Coordinator, error) {
	if len(endpoints) == 0 {
		return nil, errNoEndpointsProvided
	}
	coordinator := &m3nschCoordinator{
		opts:        opts,
		clients:     make(map[string]*m3nschClient),
		newClientFn: newM3nschClient,
	}
	return coordinator, coordinator.initializeConnections(endpoints)
}

func (m *m3nschCoordinator) initializeConnections(endpoints []string) error {
	var (
		wg           sync.WaitGroup
		numEndpoints = len(endpoints)
		multiErr     syncClientMultiErr
	)

	wg.Add(numEndpoints)
	for i := 0; i < numEndpoints; i++ {
		go func(idx int) {
			defer wg.Done()
			endpoint := endpoints[idx]
			client, err := m.newClientFn(endpoint, m.opts.InstrumentOptions(), m.opts.Timeout())
			if err != nil {
				multiErr.Add(endpoint, err)
				return
			}
			m.Lock()
			m.clients[endpoint] = client
			m.Unlock()
		}(i)
	}
	wg.Wait()

	return multiErr.FinalError()
}

func (m *m3nschCoordinator) Teardown() error {
	var err syncClientMultiErr
	m.forEachClient(func(c *m3nschClient) {
		err.Add(c.endpoint, c.close())
	})
	return err.FinalError()
}

func (m *m3nschCoordinator) Status() (map[string]m3nsch.AgentStatus, error) {
	var (
		ctx      = context.Background()
		multiErr syncClientMultiErr

		lock     sync.Mutex
		statuses = make(map[string]m3nsch.AgentStatus, len(m.clients))
	)

	m.forEachClient(func(c *m3nschClient) {
		response, err := c.client.Status(ctx, &rpc.StatusRequest{})
		if err != nil {
			multiErr.Add(c.endpoint, err)
			return
		}

		status, err := convert.ToM3nschStatus(response.Status)
		if err != nil {
			multiErr.Add(c.endpoint, err)
			return
		}

		workload, err := convert.ToM3nschWorkload(response.GetWorkload())
		if err != nil {
			multiErr.Add(c.endpoint, err)
			return
		}

		lock.Lock()
		statuses[c.endpoint] = m3nsch.AgentStatus{
			Status:   status,
			Token:    response.Token,
			MaxQPS:   response.MaxQPS,
			Workload: workload,
		}
		lock.Unlock()
	})

	return statuses, nil
}

func (m *m3nschCoordinator) Workload() (m3nsch.Workload, error) {
	statuses, err := m.Status()
	if err != nil {
		return m3nsch.Workload{}, err
	}

	// ensure all agents are initialized
	var multiErr syncClientMultiErr
	for endpoint, status := range statuses {
		if status.Status == m3nsch.StatusUninitialized {
			multiErr.Add(endpoint, fmt.Errorf("agent not initialized"))
		}
	}
	if err = multiErr.FinalError(); err != nil {
		return m3nsch.Workload{}, err
	}

	// TODO(prateek): ensure all agent workloads are set the same
	// TODO(prateek): ensure no agent have overlapping metric ranges
	var (
		workload = m3nsch.Workload{}
		first    = true
	)
	for _, status := range statuses {
		if first {
			workload.BaseTime = status.Workload.BaseTime
			workload.Namespace = status.Workload.Namespace
			workload.MetricPrefix = status.Workload.MetricPrefix
			first = false
		}
		workload.Cardinality += status.Workload.Cardinality
		workload.IngressQPS += status.Workload.IngressQPS
	}

	return workload, nil
}

func (m *m3nschCoordinator) SetWorkload(workload m3nsch.Workload) error {
	statuses, err := m.Status()
	if err != nil {
		return err
	}

	// ensure all agents are initialized
	var multiErr syncClientMultiErr
	for endpoint, status := range statuses {
		if status.Status == m3nsch.StatusUninitialized {
			multiErr.Add(endpoint, fmt.Errorf("agent not initialized"))
		}
	}
	if err = multiErr.FinalError(); err != nil {
		return err
	}

	// split workload amongst the various agents
	splitWorkload, err := m.splitWorkload(workload, statuses)
	if err != nil {
		return err
	}

	// set the targetWorkload on each agent
	ctx := context.Background()
	multiErr = syncClientMultiErr{}
	m.forEachClient(func(c *m3nschClient) {
		endpoint := c.endpoint
		targetWorkload, ok := splitWorkload[endpoint]
		if !ok {
			multiErr.Add(endpoint, fmt.Errorf("splitWorkload does not contain all endpoints"))
			return
		}

		rpcWorkload := convert.ToProtoWorkload(targetWorkload)
		_, clientErr := c.client.Modify(ctx, &rpc.ModifyRequest{Workload: &rpcWorkload})
		multiErr.Add(endpoint, clientErr)
	})

	return multiErr.FinalError()
}

func (m *m3nschCoordinator) Init(
	token string,
	workload m3nsch.Workload,
	force bool,
	targetZone string,
	targetEnv string,
) error {
	statuses, err := m.Status()
	if err != nil {
		return err
	}

	// ensure all agents are not initialized
	var multiErr syncClientMultiErr
	for endpoint, status := range statuses {
		if status.Status != m3nsch.StatusUninitialized {
			multiErr.Add(endpoint, fmt.Errorf("agent already initialized"))
		}
	}
	if err = multiErr.FinalError(); err != nil {
		return err
	}

	// split workload amongst the various agents
	splitWorkload, err := m.splitWorkload(workload, statuses)
	if err != nil {
		return err
	}

	// initialize each agent with targetWorkload
	ctx := context.Background()
	multiErr = syncClientMultiErr{}
	m.forEachClient(func(c *m3nschClient) {
		endpoint := c.endpoint
		targetWorkload, ok := splitWorkload[endpoint]
		if !ok {
			multiErr.Add(endpoint, fmt.Errorf("splitWorkload does not contain all endpoints"))
			return
		}

		rpcWorkload := convert.ToProtoWorkload(targetWorkload)
		_, clientErr := c.client.Init(ctx, &rpc.InitRequest{
			Token:      token,
			Workload:   &rpcWorkload,
			Force:      force,
			TargetZone: targetZone,
			TargetEnv:  targetEnv,
		})
		multiErr.Add(endpoint, clientErr)
	})

	return multiErr.FinalError()
}

func (m *m3nschCoordinator) Start() error {
	statuses, err := m.Status()
	if err != nil {
		return err
	}

	// ensure all agents are initialized
	var multiErr syncClientMultiErr
	for endpoint, status := range statuses {
		if status.Status == m3nsch.StatusUninitialized {
			multiErr.Add(endpoint, fmt.Errorf("agent not initialized"))
		}
	}
	if err = multiErr.FinalError(); err != nil {
		return err
	}

	// start each agent
	ctx := context.Background()
	multiErr = syncClientMultiErr{}
	m.forEachClient(func(c *m3nschClient) {
		endpoint := c.endpoint
		_, clientErr := c.client.Start(ctx, &rpc.StartRequest{})
		multiErr.Add(endpoint, clientErr)
	})

	return multiErr.FinalError()
}

func (m *m3nschCoordinator) Stop() error {
	// stop each agent
	ctx := context.Background()
	multiErr := syncClientMultiErr{}
	m.forEachClient(func(c *m3nschClient) {
		endpoint := c.endpoint
		_, err := c.client.Stop(ctx, &rpc.StopRequest{})
		multiErr.Add(endpoint, err)
	})

	return multiErr.FinalError()
}

func (m *m3nschCoordinator) splitWorkload(
	aggWorkload m3nsch.Workload,
	statuses map[string]m3nsch.AgentStatus,
) (map[string]m3nsch.Workload, error) {
	// ensure we have enough aggregate capacity to satisfy workload
	totalIngressCapacity := int64(0)
	for _, status := range statuses {
		totalIngressCapacity += status.MaxQPS
	}
	if totalIngressCapacity < int64(aggWorkload.IngressQPS) {
		return nil, fmt.Errorf("insufficient capacity")
	}

	// initialiaze return map
	splitWorkload := make(map[string]m3nsch.Workload, len(statuses))

	// split workload per worker proportional to capacity
	metricStart := 0
	for endpoint, status := range statuses {
		var (
			workerFrac     = float64(status.MaxQPS) / float64(totalIngressCapacity)
			numMetrics     = int(float64(aggWorkload.Cardinality) * workerFrac)
			qps            = int(float64(aggWorkload.IngressQPS) * workerFrac)
			workerWorkload = aggWorkload
		)
		workerWorkload.MetricStartIdx = metricStart
		workerWorkload.Cardinality = numMetrics
		workerWorkload.IngressQPS = qps
		splitWorkload[endpoint] = workerWorkload

		metricStart += numMetrics
	}
	return splitWorkload, nil
}

type syncClientMultiErr struct {
	sync.Mutex
	multiErr xerrors.MultiError
}

func (e *syncClientMultiErr) Add(endpoint string, err error) {
	if err == nil {
		return
	}
	cErr := fmt.Errorf("[%v] %v", endpoint, err)
	e.Lock()
	e.multiErr = e.multiErr.Add(cErr)
	e.Unlock()
}

func (e *syncClientMultiErr) FinalError() error {
	return e.multiErr.FinalError()
}

type forEachClientFn func(client *m3nschClient)

func (m *m3nschCoordinator) forEachClient(fn forEachClientFn) {
	var (
		parallel   = m.opts.ParallelOperations()
		numClients = len(m.clients)
		wg         sync.WaitGroup
	)

	if parallel {
		wg.Add(numClients)
	}

	for _, client := range m.clients {
		if parallel {
			go func(c *m3nschClient) {
				defer wg.Done()
				fn(c)
			}(client)
		} else {
			fn(client)
		}
	}

	if parallel {
		wg.Wait()
	}
}

type newM3nschClientFn func(string, instrument.Options, time.Duration) (*m3nschClient, error)

type m3nschClient struct {
	endpoint string
	logger   xlog.Logger
	conn     *grpc.ClientConn
	client   rpc.MenschClient
}

func newM3nschClient(
	endpoint string,
	iopts instrument.Options,
	timeout time.Duration,
) (*m3nschClient, error) {
	var (
		logger    = iopts.Logger().WithFields(xlog.NewField("client", endpoint))
		conn, err = grpc.Dial(endpoint, grpc.WithTimeout(timeout), grpc.WithInsecure())
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	logger.Debug("connection established")
	client := rpc.NewMenschClient(conn)
	return &m3nschClient{
		endpoint: endpoint,
		logger:   logger,
		conn:     conn,
		client:   client,
	}, nil
}

func (mc *m3nschClient) close() error {
	return mc.conn.Close()
}
