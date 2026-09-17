package policy

import (
	"fmt"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
)

// RoutingPolicy is a policy for routing metrics to a consumer service.
type RoutingPolicy struct {
	TrafficTypes uint64 // bitmask of traffic types to route
}

// NewRoutingPolicy creates a new routing policy.
func NewRoutingPolicy(trafficTypes uint64) RoutingPolicy {
	return RoutingPolicy{TrafficTypes: trafficTypes}
}

// String returns a string representation of the routing policy.
func (p *RoutingPolicy) String() string {
	return fmt.Sprintf("%d", p.TrafficTypes)
}

// ToProto converts the routing policy to a protobuf message in place.
func (p *RoutingPolicy) ToProto(pb *policypb.RoutingPolicy) error {
	pb.TrafficTypes = p.TrafficTypes
	return nil
}

// FromProto converts the protobuf message to a routing policy in place.
func (p *RoutingPolicy) FromProto(pb policypb.RoutingPolicy) error {
	p.TrafficTypes = pb.TrafficTypes
	return nil
}

// Equal returns true if the routing policy is equal to the other routing policy.
func (p *RoutingPolicy) Equal(other *RoutingPolicy) bool {
	return p.TrafficTypes == other.TrafficTypes
}

// Clone clones the routing policy.
func (p *RoutingPolicy) Clone() RoutingPolicy {
	return RoutingPolicy{
		TrafficTypes: p.TrafficTypes,
	}
}
