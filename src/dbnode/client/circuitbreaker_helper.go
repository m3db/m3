package client

import (
	"fmt"

	"github.com/m3db/m3/src/cluster/kv"
	cb "github.com/m3db/m3/src/dbnode/client/circuitbreaker/middleware"
	"github.com/m3db/m3/src/x/instrument"
)

// SetupCircuitBreakerProvider sets up the circuit breaker middleware provider
// with a direct KV store reference. This function can be called when you have access
// to the KV store to ensure circuit breaker middleware is properly configured.
func SetupCircuitBreakerProvider(
	kvStore kv.Store,
	iopts instrument.Options,
) (cb.EnableProvider, error) {
	if kvStore != nil {
		// Create a single provider instance
		provider := cb.NewEnableProvider()

		// Set up circuit breaker middleware config watch
		if err := provider.WatchConfig(kvStore, iopts.Logger()); err != nil {
			return nil, fmt.Errorf("failed to set up circuit breaker middleware config watch: %w", err)
		}

		return provider, nil
	} else {
		// If no KV store is available, use a nop provider to prevent nil pointer dereference
		iopts.Logger().Info("no KV store is available, using nop provider")
		return cb.NewNopEnableProvider(), nil
	}
}
