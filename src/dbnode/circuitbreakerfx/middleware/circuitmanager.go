package middleware

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
	"sync"
)

// circuitManager manages creation and reuse of the circuits across repeated
// service::procedure calls.
type circuitManager struct {
	mu             sync.RWMutex
	policyProvider *policyProvider
	circuitCache   map[serviceProcedure]circuitbreaker.Circuiter
}

func newCircuitManager(policyProvider *policyProvider) *circuitManager {
	return &circuitManager{
		policyProvider: policyProvider,
		circuitCache:   make(map[serviceProcedure]circuitbreaker.Circuiter),
	}
}

// circuit returns a circuit for the given service and procedure.
// If circuit is unavailable in the cache, it tries creating the circuit if the
// policy for this service and procedure is available.
// Return nil if the circuit is unavailable.
// This method will return an error only when circuit breaker circuit creation
// results in an error.
func (c *circuitManager) circuit(service, procedure string) (circuitbreaker.Circuiter, error) {
	key := serviceProcedure{service: service, procedure: procedure}
	c.mu.RLock()
	circuit, ok := c.circuitCache[key]
	c.mu.RUnlock()
	if ok {
		return circuit, nil
	}

	config, ok := c.policyProvider.policy(service, procedure)
	if !ok {
		c.mu.Lock()
		// When the config for a service and procedure is not found, set the circuit
		// cache to nil to avoid recurring config lookup again in future for same
		// service and procedure.
		c.circuitCache[key] = nil
		c.mu.Unlock()
		return nil, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Check again if the circuit has been created concurrently.
	if circuit, ok := c.circuitCache[key]; ok {
		return circuit, nil
	}

	circuit, err := circuitbreaker.NewCircuit(config)
	if err != nil {
		// Set the circuit cache for a service and procedure to nil, to avoid
		// recreating a circuit which would always result in an error.
		c.circuitCache[key] = nil
		return nil, err
	}

	c.circuitCache[key] = circuit
	return circuit, nil
}

// walk invokes the given method with every circuit breaker present in the cache
// of the circuit manager.
func (c *circuitManager) walk(handler func(serviceProcedure, circuitbreaker.Circuiter)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for key, circuit := range c.circuitCache {
		handler(key, circuit)
	}
}

// serviceProcedure is a helper struct used as the key of the map.
type serviceProcedure struct {
	service   string
	procedure string
}
