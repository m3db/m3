package routing

import (
	"github.com/m3db/m3/src/cluster/kv"
	routingpolicypb "github.com/m3db/m3/src/msg/generated/proto/routingpolicypb"
)

// Policy is a policy for routing metrics to a consumer service.
type Policy interface {
	TrafficTypes() map[string]uint64
}

// policy is a policy for routing metrics to a consumer service.
type policy struct {
	trafficTypes map[string]uint64
}

// TrafficTypes returns the traffic types.
func (p *policy) TrafficTypes() map[string]uint64 {
	return p.trafficTypes
}

// NewPolicyFromValue creates a new policy from a value, where the value is a protobuf message of RoutingPolicyConfig.
func NewPolicyFromValue(v kv.Value) (Policy, error) {
	var protoPolicy routingpolicypb.RoutingPolicyConfig
	if err := v.Unmarshal(&protoPolicy); err != nil {
		return nil, err
	}
	return &policy{
		trafficTypes: protoPolicy.TrafficTypes,
	}, nil
}
