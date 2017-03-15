package coordinator

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3nsch"

	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/require"
)

var (
	testEndpoints = []string{
		"testEndpoint1",
		"testEndpoint2",
	}
)

func newTestCoordinator() *m3nschCoordinator {
	iopts := instrument.NewOptions()
	opts := NewOptions(iopts)
	return &m3nschCoordinator{
		opts:    opts,
		clients: make(map[string]*m3nschClient),
	}
}

func TestInitializeConnections(t *testing.T) {
	var (
		lock        sync.Mutex
		initMap     = make(map[string]bool)
		coordinator = newTestCoordinator()
	)
	coordinator.newClientFn = func(e string, _ instrument.Options, _ time.Duration) (*m3nschClient, error) {
		lock.Lock()
		initMap[e] = true
		lock.Unlock()
		return &m3nschClient{}, nil
	}

	err := coordinator.initializeConnections(testEndpoints)
	require.NoError(t, err)

	for _, endpoint := range testEndpoints {
		flag, ok := initMap[endpoint]
		require.True(t, ok, "endpoint not initialized: %v", endpoint)
		require.True(t, flag, "endpoint not initialized: %v", endpoint)
	}
}

func TestForEachClient(t *testing.T) {
	coordinator := newTestCoordinator()
	coordinator.newClientFn = func(e string, _ instrument.Options, _ time.Duration) (*m3nschClient, error) {
		return &m3nschClient{endpoint: e}, nil
	}
	err := coordinator.initializeConnections(testEndpoints)
	require.NoError(t, err)

	// non-parallel version
	coordinator.opts = coordinator.opts.SetParallelOperations(false)
	clientMap := make(map[string]bool)
	coordinator.forEachClient(func(c *m3nschClient) {
		clientMap[c.endpoint] = true
	})
	for _, endpoint := range testEndpoints {
		flag, ok := clientMap[endpoint]
		require.True(t, ok, "endpoint not initialized: %v", endpoint)
		require.True(t, flag, "endpoint not initialized: %v", endpoint)
	}

	// parallel version
	var lock sync.Mutex
	coordinator.opts = coordinator.opts.SetParallelOperations(true)
	clientMap = make(map[string]bool)
	coordinator.forEachClient(func(c *m3nschClient) {
		lock.Lock()
		clientMap[c.endpoint] = true
		lock.Unlock()
	})
	for _, endpoint := range testEndpoints {
		flag, ok := clientMap[endpoint]
		require.True(t, ok, "endpoint not initialized: %v", endpoint)
		require.True(t, flag, "endpoint not initialized: %v", endpoint)
	}
}

func TestSplitWorkloadFail(t *testing.T) {
	coordinator := newTestCoordinator()
	aggregateWorkload := m3nsch.Workload{
		IngressQPS: 2,
	}
	statuses := map[string]m3nsch.AgentStatus{
		testEndpoints[0]: {
			MaxQPS: 1,
		},
	}
	_, err := coordinator.splitWorkload(aggregateWorkload, statuses)
	require.Error(t, err)
}

func TestSplitWorkload(t *testing.T) {
	coordinator := newTestCoordinator()
	aggregateWorkload := m3nsch.Workload{
		Cardinality: 3000,
		IngressQPS:  300,
	}
	statuses := map[string]m3nsch.AgentStatus{
		testEndpoints[0]: {
			MaxQPS: 200,
		},
		testEndpoints[1]: {
			MaxQPS: 400,
		},
	}
	splitWorkloads, err := coordinator.splitWorkload(aggregateWorkload, statuses)
	require.NoError(t, err)
	require.Equal(t, 2, len(splitWorkloads))

	workload1, ok := splitWorkloads[testEndpoints[0]]
	require.True(t, ok)
	require.Equal(t, 1000, workload1.Cardinality)
	require.Equal(t, 100, workload1.IngressQPS)

	workload2, ok := splitWorkloads[testEndpoints[1]]
	require.True(t, ok)
	require.Equal(t, 2000, workload2.Cardinality)
	require.Equal(t, 200, workload2.IngressQPS)
}
