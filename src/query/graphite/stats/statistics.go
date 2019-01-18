package stats

import (
	"math"
)

// Values presents a set of data values as an array, for the purposes of aggregation
type Values interface {
	// Len returns the number of values present
	Len() int

	// ValueAt returns the value at the nth element
	ValueAt(n int) float64
}

// MutableValues is a set of data values that can be modified
type MutableValues interface {
	Values

	// SetValueAt sets the value at the nth element
	SetValueAt(n int, v float64)
}

// Float64Values is a simple Values implementation around a slice
type Float64Values []float64

// Len returns the number of elements in the array
func (vals Float64Values) Len() int { return len(vals) }

// ValueAt returns the value at the nth element
func (vals Float64Values) ValueAt(n int) float64 { return vals[n] }

// SetValueAt sets the value at the nth element
func (vals Float64Values) SetValueAt(n int, v float64) { vals[n] = v }

// Statistics are the computation of standard statistics (min, max, mean, count, stddev)
// over a group of values.
type Statistics struct {
	Min    float64
	Max    float64
	Mean   float64
	Count  uint
	Sum    float64
	StdDev float64
}

// Merge merges a group of statistics
func Merge(statistics []Statistics) Statistics {
	var (
		count               uint
		min, max, mean, sum float64
	)

	for _, a := range statistics {
		if a.Count == 0 {
			continue
		}

		if count == 0 {
			min, max = a.Min, a.Max
		} else {
			min, max = math.Min(min, a.Min), math.Max(max, a.Max)
		}

		priorCount := count
		count += a.Count
		sum += a.Sum
		mean = ((a.Mean * float64(a.Count)) + (mean * float64(priorCount))) / float64(count)
	}

	if count == 0 {
		return Statistics{}
	}

	var sum1, sum2 float64
	for _, a := range statistics {
		if a.Count == 0 {
			continue
		}

		variance := a.StdDev * a.StdDev
		avg := a.Mean
		sum1 += float64(a.Count) * variance
		sum2 += float64(a.Count) * math.Pow(avg-mean, 2)
	}

	variance := ((sum1 + sum2) / float64(count))
	return Statistics{
		Count:  count,
		Min:    min,
		Max:    max,
		Mean:   mean,
		Sum:    sum,
		StdDev: math.Sqrt(variance),
	}
}

func calc(values Values) (uint, float64, float64, float64, float64, float64) {
	count := uint(0)
	sum := float64(0)
	min := math.MaxFloat64
	max := -math.MaxFloat64
	for i := 0; i < values.Len(); i++ {
		n := values.ValueAt(i)
		if math.IsNaN(n) {
			continue
		}
		count++
		sum += n
		min = math.Min(n, min)
		max = math.Max(n, max)
	}

	if count == 0 {
		nan := math.NaN()
		return 0, nan, nan, nan, nan, nan
	}

	mean := float64(0)
	if count > 0 {
		mean = sum / float64(count)
	}

	stddev := float64(0)
	if count > 1 {
		m2 := float64(0)
		for i := 0; i < values.Len(); i++ {
			n := values.ValueAt(i)
			if math.IsNaN(n) {
				continue
			}

			diff := n - mean
			m2 += diff * diff
		}

		variance := m2 / float64(count-1)
		stddev = math.Sqrt(variance)
	}
	return count, min, max, mean, sum, stddev
}

// Calc calculates statistics for a set of values
func Calc(values Values) Statistics {
	count, min, max, mean, sum, stddev := calc(values)
	return Statistics{
		Count:  count,
		Min:    min,
		Max:    max,
		Mean:   mean,
		Sum:    sum,
		StdDev: stddev,
	}
}

// SingleCountStatistics returns Statistics for a single value
func SingleCountStatistics(value float64) Statistics {
	return Statistics{
		Count:  1,
		Min:    value,
		Max:    value,
		Sum:    value,
		Mean:   value,
		StdDev: 0,
	}
}

// ZeroCountStatistics returns statistics when no values are present
// (or when all values are NaNs)
func ZeroCountStatistics() Statistics {
	nan := math.NaN()
	return Statistics{
		Count:  0,
		Min:    nan,
		Max:    nan,
		Sum:    nan,
		Mean:   nan,
		StdDev: nan,
	}
}
