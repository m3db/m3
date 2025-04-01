package middleware

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreaker"
	"go.uber.org/net/metrics"
	// "go.uber.org/yarpc/api/transport"
	// "go.uber.org/yarpc/yarpcerrors"
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
	scope *metrics.Scope

	// circuitStateHeartbeat is a counter used for emitting circuit state such as circuit
	// state and probe-ratio (if in probing state).
	circuitStateHeartbeat *metrics.CounterVector

	// callEdges is a cache of edges created for previously seen requests.
	callEdges map[edgeKey]*callEdge
}

func newObserver(serviceName string, scope *metrics.Scope) (*observer, error) {
	circuitHeartbeat, err := scope.CounterVector(metrics.Spec{
		Name:      "circuit_breaker_heartbeat",
		Help:      "Periodic counter used to report the state of the circuit breaker.",
		ConstTags: metrics.Tags{"source": serviceName, "component": _packageName},
		VarTags:   []string{_tagDest, _tagProcedure, _tagState, _tagProbeRatio, _tagMode},
	})
	if err != nil {
		return nil, err
	}

	return &observer{
		circuitStateHeartbeat: circuitHeartbeat,
		scope:                 scope,
		callEdges:             make(map[edgeKey]*callEdge, _observerCacheSize),
	}, nil
}

// reportCircuitHeartbeat increments the circuit state heartbeat for the given
// service and procedure provided.
func (o *observer) reportCircuitStateHeartbeat(status *circuitbreaker.Status, service, procedure string, mode Mode) {
	o.circuitStateHeartbeat.MustGet(
		_tagDest, service,
		_tagProcedure, procedure,
		_tagState, status.State().String(),
		_tagProbeRatio, probeRatioString(status.ProbeRatio()),
		_tagMode, mode.String(),
	).Inc()
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
	successes *metrics.CounterVector

	// Failures counter that counts the number of failed requests.
	failures *metrics.CounterVector

	// Rejects counter that counts the number of rejected requests.
	rejects *metrics.CounterVector
}

// newCallEdge creates an edge for the given request.
func newCallEdge(scope *metrics.Scope, host string) (*callEdge, error) {
	tags := metrics.Tags{
		"component": _packageName,
		"host":      host,
	}

	successes, err := scope.CounterVector(metrics.Spec{
		Name:      "circuit_breaker_successes",
		Help:      "Counter of number of successful requests in the circuit breaker.",
		ConstTags: tags,
		VarTags:   []string{_tagState, _tagProbeRatio, _tagMode},
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to create successes counter: %w", err)
	}

	failures, err := scope.CounterVector(metrics.Spec{
		Name:      "circuit_breaker_failures",
		Help:      "Counter of number of failed requests in the circuit breaker.",
		ConstTags: tags,
		VarTags:   []string{_tagState, _tagProbeRatio, _tagErrorCode, _tagMode},
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to create failures counter: %w", err)
	}

	rejects, err := scope.CounterVector(metrics.Spec{
		Name:      "circuit_breaker_rejects",
		Help:      "Counter of number of rejected requests in the circuit breaker.",
		ConstTags: tags,
		VarTags:   []string{_tagState, _tagProbeRatio, _tagMode},
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to create rejects counter: %w", err)
	}

	return &callEdge{
		successes: successes,
		failures:  failures,
		rejects:   rejects,
	}, nil
}

// reportRequestComplete increments success counter if request is successful
// or increments failure counter with state, probe-ratio and error code.
func (c *callEdge) reportRequestComplete(status *circuitbreaker.Status, isRequestSuccessful bool, respErr error, mode Mode) {
	state := status.State().String()
	probeRatio := probeRatioString(status.ProbeRatio())
	if isRequestSuccessful {
		c.successes.MustGet(
			_tagState, state,
			_tagProbeRatio, probeRatio,
			_tagMode, mode.String(),
		).Inc()
		return
	}

	c.failures.MustGet(
		_tagState, state,
		_tagProbeRatio, probeRatio,
		_tagErrorCode, respErr.Error(),
		_tagMode, mode.String(),
	).Inc()
}

// reportRequestRejected increments request rejected metric with circuit state, probe ratio
// and mode of the middleware.
func (c *callEdge) reportRequestRejected(status *circuitbreaker.Status, mode Mode) {
	c.rejects.MustGet(
		_tagState, status.State().String(),
		_tagProbeRatio, probeRatioString(status.ProbeRatio()),
		_tagMode, mode.String(),
	).Inc()
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
