package circuitbreaker

// State type represents the states of the circuit breaker.
type State int

const (
	// Healthy circuit breaker state allows all the requests.
	Healthy State = iota + 1
	// Unhealthy circuit breaker state rejects all the requests.
	Unhealthy
	// Probing circuit breaker state partially accepts and probes requests, rest are rejected.
	Probing
)

// String returns a lower-case ASCII representation of the State.
func (s State) String() string {
	switch s {
	case Healthy:
		return "healthy"
	case Unhealthy:
		return "unhealthy"
	case Probing:
		return "probing"
	default:
		return "unknown"
	}
}
