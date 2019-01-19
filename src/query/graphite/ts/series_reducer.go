package ts

import "fmt"

// SeriesReducerApproach defines an approach to reduce a series to a single value.
type SeriesReducerApproach string

// The standard set of reducers
const (
	SeriesReducerAvg    SeriesReducerApproach = "avg"
	SeriesReducerSum    SeriesReducerApproach = "total"
	SeriesReducerMin    SeriesReducerApproach = "min"
	SeriesReducerMax    SeriesReducerApproach = "max"
	SeriesReducerStdDev SeriesReducerApproach = "stddev"
	SeriesReducerLast   SeriesReducerApproach = "last"
)

// SeriesReducer reduces a series to a single value.
type SeriesReducer func(*Series) float64

// SafeReducer returns a boolean indicating whether it is a valid reducer,
// and if so, the SeriesReducer implementing the SeriesReducerApproach.
func (sa SeriesReducerApproach) SafeReducer() (SeriesReducer, bool) {
	r, ok := seriesReducers[sa]
	return r, ok
}

// Reducer returns the SeriesReducer implementing the SeriesReducerApproach.
func (sa SeriesReducerApproach) Reducer() SeriesReducer {
	r, ok := sa.SafeReducer()
	if !ok {
		panic(fmt.Sprintf("No reducer func for %s", sa))
	}
	return r
}

var seriesReducers = map[SeriesReducerApproach]SeriesReducer{
	SeriesReducerAvg:    func(b *Series) float64 { return b.SafeAvg() },
	SeriesReducerSum:    func(b *Series) float64 { return b.SafeSum() },
	SeriesReducerMin:    func(b *Series) float64 { return b.SafeMin() },
	SeriesReducerMax:    func(b *Series) float64 { return b.SafeMax() },
	SeriesReducerStdDev: func(b *Series) float64 { return b.SafeStdDev() },
	SeriesReducerLast:   func(b *Series) float64 { return b.SafeLastValue() },
}
