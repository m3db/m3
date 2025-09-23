package policy

import (
	"fmt"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
)

// RoutePolicy is a policy for routing metrics to a consumer service.
type RoutePolicy struct {
	TrafficTypes uint64 // bitmask of traffic types to route
}

// NewRoutePolicy creates a new route policy.
func NewRoutePolicy(trafficTypes uint64) RoutePolicy {
	return RoutePolicy{TrafficTypes: trafficTypes}
}

// String returns a string representation of the route policy.
func (p *RoutePolicy) String() string {
	return fmt.Sprintf("%d", p.TrafficTypes)
}

// ToProto converts the route policy to a protobuf message in place.
func (p *RoutePolicy) ToProto(pb *policypb.RoutePolicy) error {
	pb.TrafficTypes = p.TrafficTypes
	return nil
}

// FromProto converts the protobuf message to a route policy in place.
func (p *RoutePolicy) FromProto(pb policypb.RoutePolicy) error {
	p.TrafficTypes = pb.TrafficTypes
	return nil
}

// Equal returns true if the route policy is equal to the other route policy.
func (p *RoutePolicy) Equal(other *RoutePolicy) bool {
	return p.TrafficTypes == other.TrafficTypes
}
