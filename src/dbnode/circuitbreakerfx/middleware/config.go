package middleware

import (
	"context"
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
)

const (
	// AllServicesWildcard special name for specifying all the services.
	AllServicesWildcard = "*"
)

// Enabler exposes method to check when the middleware is enabled.
type Enabler interface {
	// IsEnabled returns true when middleware is enabled for the given service
	// and procedure.
	IsEnabled(ctx context.Context, service, procedure string) bool

	// Mode returns the mode of the middleware for the given service and procedure.
	Mode(ctx context.Context, service, procedure string) Mode
}

// PolicyOverride defines override circuit breaker policy for a Service or
// a Service and Procedure combination.
type PolicyOverride struct {
	// Service is an outbound YARPC service name.
	Service string `yaml:"service"`

	// Procedure is an outbound YARPC procedure name.
	Procedure string `yaml:"procedure"`

	// WithPolicy specifies the policy name to use for the override. It MUST
	// reference an existing policy.
	WithPolicy string `yaml:"with"`
}

// Config is a definition of the circuit breaker middleware.
type Config struct {

	// Enable must be set to true to enable circuit breaker middleware.
	Enable bool `yaml:"enable"`

	// EnableDefaultPolicy when set enables circuit breaker middleware for all the
	// outbound calls and applies the default policy.
	EnableDefaultPolicy bool `yaml:"enableDefaultPolicy"`

	// EnableShadowMode when set enables shadow mode where outbound requests are not
	// blocked when circuit breaker rejects the request.
	// deprecated, use "shadow" instead
	EnableShadowMode bool `yaml:"enableShadowMode"`

	// DefaultPolicy is the name of the default policy. It must reference a name from
	// the given Policies.
	DefaultPolicy string `yaml:"defaultPolicy"`

	// Policies is a map of user defined circuit breaker configs.
	Policies map[string]circuitbreaker.Config `yaml:"policies"`

	// Overrides allow changing the circuit breaker policy for a matching service
	// or service+procedure.
	Overrides []PolicyOverride `yaml:"overrides"`
}
