package routing

import (
	"github.com/m3db/m3/src/cluster/kv"
	routingpolicypb "github.com/m3db/m3/src/msg/generated/proto/routingpolicypb"
)

// PolicyConfig contains information for how to route metrics to a consumer service.
type PolicyConfig interface {
	TrafficTypes() map[string]uint64
}

type policyConfig struct {
	trafficTypes map[string]uint64
}

// TrafficTypes returns the traffic types.
func (p *policyConfig) TrafficTypes() map[string]uint64 {
	return p.trafficTypes
}

// NewPolicyConfig creates a new policy config.
func NewPolicyConfig(trafficTypes map[string]uint64) PolicyConfig {
	return &policyConfig{
		trafficTypes: trafficTypes,
	}
}

// NewPolicyConfigFromValue creates a new policy from a value, where the value is a protobuf message of RoutingPolicyConfig.
func NewPolicyConfigFromValue(v kv.Value) (PolicyConfig, error) {
	var protoPolicy routingpolicypb.RoutingPolicyConfig
	if err := v.Unmarshal(&protoPolicy); err != nil {
		return nil, err
	}
	return NewPolicyConfig(protoPolicy.TrafficTypes), nil
}
