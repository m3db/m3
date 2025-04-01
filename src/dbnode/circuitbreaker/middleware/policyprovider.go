package middleware

import (
	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreaker"
)

// policyProvider provides circuit policy for service and procedure combination.
type policyProvider struct {
	enableDefaultPolicy bool
	defaultPolicy       circuitbreaker.Config
	policyOverrides     map[serviceProcedure]circuitbreaker.Config
}

func newPolicyProvider(config Config) *policyProvider {
	overrides := make(map[serviceProcedure]circuitbreaker.Config)
	for _, override := range config.Overrides {
		serviceProcedureKey := serviceProcedure{service: override.Service, procedure: override.Procedure}
		overrides[serviceProcedureKey] = config.Policies[override.WithPolicy]
	}

	defaultPolicy := circuitbreaker.Config{}
	if config.DefaultPolicy != "" {
		defaultPolicy = config.Policies[config.DefaultPolicy]
	}

	return &policyProvider{
		enableDefaultPolicy: config.EnableDefaultPolicy,
		defaultPolicy:       defaultPolicy,
		policyOverrides:     overrides,
	}
}

// policy returns circuit breaker config for the given service and procedure
// Method looks for the config in multiple steps:
// 1) Find (service,procedure) config override in overrides cache.
// 2) Find (service) config override in overrides cache.
// 3) If default policy is enabled, return default policy.
// If config is not found, second return value is false.
func (p *policyProvider) policy(service, procedure string) (circuitbreaker.Config, bool) {
	// Level-1: Find (service,procedure) config override in overrides cache.
	if config, ok := p.policyOverrides[serviceProcedure{service: service, procedure: procedure}]; ok {
		return config, true
	}

	// Level-2: Find (service) config override in overrides cache.
	if config, ok := p.policyOverrides[serviceProcedure{service: service}]; ok {
		return config, true
	}

	// Level-3: If default policy is enabled, return default policy.
	if p.enableDefaultPolicy {
		return p.defaultPolicy, true
	}

	return circuitbreaker.Config{}, false
}
