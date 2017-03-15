package datums

// TSGenFn represents a pure function used to
// generate synthetic time series'
type TSGenFn func(idx int) float64

// SyntheticTimeSeries represents a synthetically generated
// time series
type SyntheticTimeSeries interface {
	// ID returns the id of the SyntheticTimeSeries
	ID() int

	// Size returns the number of points in the SyntheticTimeSeries
	Size() int

	// Data returns data points comprising the SyntheticTimeSeries
	Data() []float64

	// Get(n) returns the nth (circularly wrapped) data point
	Get(n int) float64

	// Next simulates an infinite iterator on the SyntheticTimeSeries
	Next() float64
}

// Registry is a collection of synthetic time series'
type Registry interface {
	// Get(n) returns the nth (wrapped circularly) SyntheticTimeSeries
	// known to the Registry.
	Get(n int) SyntheticTimeSeries

	// Size returns the number of unique time series'
	// the Registry is capable of generating.
	Size() int
}
