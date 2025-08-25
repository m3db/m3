package policy

type RoutePolicy struct {
	TrafficTypes uint64 // bitmask of traffic types to route
}
