package middleware

import (
	"context"
	"fmt"

	// "code.uber.internal/go/configfx.git/cfgparse"
	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreaker"
	// "go.uber.org/yarpc/yarpcerrors"
)

const (
	// AllServicesWildcard special name for specifying all the services.
	AllServicesWildcard = "*"
)

var (
	// ErrAmbiguousShadowWildcard is an error that occurs when wildcart "*" specified for both shadowing and rejecting list of services.
	ErrAmbiguousShadowWildcard = fmt.Errorf("circuit breaker config: wildcard may be used only once, either in enable or in disable list")
)

// Enabler exposes method to check when the middleware is enabled. This interface
// allows providing dynamic config based on Flipr/UCS.
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

// ShadowConfig represents configuration for the shadow mode.
type ShadowConfig struct {
	ShadowForListed    []ShadowList `yaml:"shadowForListed"`
	NonShadowForListed []ShadowList `yaml:"nonShadowForListed"`
}

// ShadowList represents one item in the shadow config.
type ShadowList struct {
	// Service is an outbound YARPC service name.
	Service string `yaml:"service"`

	// Procedure is an outbound YARPC procedure name.
	Procedure string `yaml:"procedure"`
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

	// Shadow defines configuration for the shadow mode.
	Shadow ShadowConfig `yaml:"shadow"`

	// DefaultPolicy is the name of the default policy. It must reference a name from
	// the given Policies.
	DefaultPolicy string `yaml:"defaultPolicy"`

	// Policies is a map of user defined circuit breaker configs.
	Policies map[string]circuitbreaker.Config `yaml:"policies"`

	// Overrides allow changing the circuit breaker policy for a matching service
	// or service+procedure.
	Overrides []PolicyOverride `yaml:"overrides"`

	// FliprPropertyForEnable is the Flipr property name for middleware enabler.
	// When this is set the middleware fetches enable config from Flipr during
	// the runtime.
	FliprPropertyForEnable string `yaml:"fliprPropertyForEnable"`

	// FailureErrors is a list of error codes which are considered as an error
	// by the circuit breaker middleware. Possible error code values are listed
	// in the doc: https://engdocs.uberinternal.com/yarpc/errors/#types-of-errors
	// When this list is not provided, middleware defaults to following errors:
	// CodeInternal, CodeDeadlineExceeded, CodeUnavailable, and CodeResourceExhausted.
	// FailureErrors []yarpcerrors.Code `yaml:"failureErrors"`
}

// Validate ensures the circuit breaker middleware configurations are valid.
// func (c Config) Validate() error {
// 	if c.DefaultPolicy != "" {
// 		if _, ok := c.Policies[c.DefaultPolicy]; !ok {
// 			return fmt.Errorf(
// 				"circuit breaker config: default policy '%s' not found in the given policies",
// 				c.DefaultPolicy,
// 			)
// 		}
// 	}

// 	for _, override := range c.Overrides {
// 		if _, ok := c.Policies[override.WithPolicy]; !ok {
// 			return fmt.Errorf(
// 				"circuit breaker config: override with policy '%s' not found in the given policies",
// 				override.WithPolicy,
// 			)
// 		}
// 		if override.Service == "" {
// 			return fmt.Errorf(
// 				"circuit breaker config: override with policy '%s' must contain service name",
// 				override.WithPolicy,
// 			)
// 		}
// 	}

// 	for policyName, policy := range c.Policies {
// 		if err := policy.Validate(); err != nil {
// 			return fmt.Errorf("circuit breaker config: policy '%s' has %s", policyName, err.Error())
// 		}
// 	}

// 	for _, failureError := range c.FailureErrors {
// 		if _, err := failureError.MarshalText(); err != nil {
// 			return fmt.Errorf("circuit breaker config: invalid failure error '%s'", err.Error())
// 		}
// 	}

// 	return validateAtMostOneAllSvc(c.Shadow)
// }

func validateAtMostOneAllSvc(cfg ShadowConfig) error {
	var allSvcCnt int

	for _, svc := range cfg.ShadowForListed {
		if svc.Service == AllServicesWildcard {
			allSvcCnt++
		}
	}

	for _, svc := range cfg.NonShadowForListed {
		if svc.Service == AllServicesWildcard {
			allSvcCnt++
		}
	}

	if allSvcCnt > 1 {
		return ErrAmbiguousShadowWildcard
	}

	return nil
}
