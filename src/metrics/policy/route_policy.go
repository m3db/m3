package policy

import (
	"fmt"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
)

type RoutePolicy struct {
	TrafficTypes uint64 // bitmask of traffic types to route
}

func NewRoutePolicy(trafficTypes uint64) RoutePolicy {
	return RoutePolicy{TrafficTypes: trafficTypes}
}

func (p RoutePolicy) String() string {
	return fmt.Sprintf("%d", p.TrafficTypes)
}

func (p RoutePolicy) ToProto(pb *policypb.RoutePolicy) error {
	pb.TrafficTypes = p.TrafficTypes
	return nil
}

func (p *RoutePolicy) FromProto(pb policypb.RoutePolicy) error {
	p.TrafficTypes = pb.TrafficTypes
	return nil
}

func (p RoutePolicy) Equal(other RoutePolicy) bool {
	return p.TrafficTypes == other.TrafficTypes
}
