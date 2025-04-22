package middleware

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
	// "go.uber.org/net/metrics"
	// "go.uber.org/yarpc/api/transport"
	// "go.uber.org/yarpc/yarpcerrors"
	"github.com/uber-go/tally"
)

const (
	_packageName       = "circuit_breaker"
	_tagState          = "circuit_state"
	_tagProbeRatio     = "probe_ratio"
	_tagErrorCode      = "error_code"
	_tagDest           = "dest"
	_tagProcedure      = "procedure"
	_tagMode           = "mode"
	_observerCacheSize = 128
)

// observer handles emitting metrics of the circuit breaker.
type observer struct {
	mu    sync.RWMutex
	scope tally.Scope

	// circuitStateHeartbeat is a counter used for emitting circuit state such as circuit
	// state and probe-ratio (if in probing state).
	circuitStateHeartbeat tally.Gauge

	// callEdges is a cache of edges created for previously seen requests.
	callEdges map[edgeKey]*callEdge
}

func newObserver(host string, scope tally.Scope) (*observer, error) {
	// tags := scope.Gauge(
	// 	map[string]string{
	// 		"component": _packageName,
	// 		"host":      host,
	// 	})

	scope = scope.Tagged(map[string]string{
		"component": _packageName,
		"host":      host,
	})

	circuitHeartbeat := scope.Gauge("circuit_breaker_heartbeat")

	return &observer{
		circuitStateHeartbeat: circuitHeartbeat,
		scope:                 scope,
		callEdges:             make(map[edgeKey]*callEdge, _observerCacheSize),
	}, nil
}

// reportCircuitHeartbeat increments the circuit state heartbeat for the given
// service and procedure provided.
func (o *observer) reportCircuitStateHeartbeat(status *circuitbreaker.Status, service, procedure string, mode Mode) {
	// tags := o.scope.Tagged(
	// 	map[string]string{
	// 		"circuit_state": status.State().String(),
	// 		"mode":          mode.String(),
	// 	})

	o.circuitStateHeartbeat.Update(o.getCircuitStateHeartbeat(status.State()))
}

func (o *observer) getCircuitStateHeartbeat(s circuitbreaker.State) float64 {
	switch s {
	case circuitbreaker.Healthy:
		return 2
	case circuitbreaker.Probing:
		return 1
	case circuitbreaker.Unhealthy:
		return 0
	default:
		return -1
	}
}

// getEdge returns observer edge for the given request either from cache or
// creates a new edge.
func (o *observer) edge(host string) (*callEdge, error) {
	if edge := o.edgeFromCache(host); edge != nil {
		return edge, nil
	}
	return o.createCallEdge(host)
}

// edgeFromCache returns edge for the given edge-key from the cache or nil if
// key not found.
func (o *observer) edgeFromCache(host string) *callEdge {
	key := newEdgeKey(host)
	o.mu.RLock()
	edge := o.callEdges[key]
	o.mu.RUnlock()
	return edge
}

// createCallEdge creates an edge for the given request if not available in cache.
func (o *observer) createCallEdge(host string) (*callEdge, error) {
	key := newEdgeKey(host)

	o.mu.Lock()
	defer o.mu.Unlock()

	// Must check that edge is not created, guard against concurrent creation.
	if edge, ok := o.callEdges[key]; ok {
		return edge, nil
	}

	edge, err := newCallEdge(o.scope, host)
	if err != nil {
		return nil, err
	}

	o.callEdges[key] = edge
	return edge, nil
}

// callEdge holds the counters which can be reused between requests with similar
// request properties such as service, procedure, caller, routing-key,
// routing-delegate, and encoding.
type callEdge struct {
	// Successes counter that counts the number of successful requests.
	successes tally.Counter

	// Failures counter that counts the number of failed requests.
	failures tally.Counter

	// Rejects counter that counts the number of rejected requests.
	rejects tally.Counter
}

// newCallEdge creates an edge for the given request.
func newCallEdge(scope tally.Scope, host string) (*callEdge, error) {
	tags := scope.Tagged(
		map[string]string{
			"component": _packageName,
			"host":      host,
		})

	// successes, err := scope.CounterVector(metrics.Spec{
	// 	Name:      "circuit_breaker_successes",
	// 	Help:      "Counter of number of successful requests in the circuit breaker.",
	// 	ConstTags: tags,
	// 	VarTags:   []string{_tagState, _tagProbeRatio, _tagMode},
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("Failed to create successes counter: %w", err)
	// }

	successes := tags.Counter("circuit_breaker_successes")

	failures := tags.Counter("circuit_breaker_failures")

	// failures, err := scope.CounterVector(metrics.Spec{
	// 	Name:      "circuit_breaker_failures",
	// 	Help:      "Counter of number of failed requests in the circuit breaker.",
	// 	ConstTags: tags,
	// 	VarTags:   []string{_tagState, _tagProbeRatio, _tagErrorCode, _tagMode},
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("Failed to create failures counter: %w", err)
	// }

	rejects := tags.Counter("circuit_breaker_rejects")

	// rejects, err := scope.CounterVector(metrics.Spec{
	// 	Name:      "circuit_breaker_rejects",
	// 	Help:      "Counter of number of rejected requests in the circuit breaker.",
	// 	ConstTags: tags,
	// 	VarTags:   []string{_tagState, _tagProbeRatio, _tagMode},
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("Failed to create rejects counter: %w", err)
	// }

	return &callEdge{
		successes: successes,
		failures:  failures,
		rejects:   rejects,
	}, nil
}

// reportRequestComplete increments success counter if request is successful
// or increments failure counter with state, probe-ratio and error code.
func (c *callEdge) reportRequestComplete(status *circuitbreaker.Status, isRequestSuccessful bool, respErr error, mode Mode) {
	// state := status.State().String()
	// probeRatio := probeRatioString(status.ProbeRatio())
	if isRequestSuccessful {
		c.successes.Inc(1)
		return
	}

	c.failures.Inc(1)
}

// reportRequestRejected increments request rejected metric with circuit state, probe ratio
// and mode of the middleware.
func (c *callEdge) reportRequestRejected(status *circuitbreaker.Status, mode Mode) {
	c.rejects.Inc(1)
}

// edgeKey is an helper struct used as a map key.
type edgeKey struct {
	host string
}

func newEdgeKey(host string) edgeKey {
	return edgeKey{
		host: host,
	}
}

// probeRatioString returns the string value of probe ratio.
// Returns "0" string if status is not in probing state.
func probeRatioString(ratio float64, ok bool) string {
	if ok {
		return fmt.Sprint(ratio)
	}
	return "0"
}
