// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package temporal

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// IRateType calculates the per-second rate of increase of the time series
	// across the specified time range. This is based on the last two data points.
	IRateType = "irate"

	// IDeltaType calculates the difference between the last two values in the time series.
	// IDeltaTemporalType should only be used with gauges.
	IDeltaType = "idelta"

	// RateType calculates the per-second average rate of increase of the time series.
	RateType = "rate"

	// DeltaType calculates the difference between the first and last value of each time series.
	DeltaType = "delta"

	// IncreaseType calculates the increase in the time series.
	IncreaseType = "increase"
)

// RateProcessor is a structure containing details about the rate.
type RateProcessor struct {
	IsRate, IsCounter bool
	RateFn            RateFn
}

func (r RateProcessor) initialize(
	duration time.Duration,
	_ transform.Options,
) processor {
	return &rateNode{
		isRate:    r.IsRate,
		isCounter: r.IsCounter,
		rateFn:    r.RateFn,
		duration:  duration,
	}
}

// NewRateOpWithProcessor creates a new base temporal transform for
// the given rate processor.
func NewRateOpWithProcessor(
	args []interface{},
	opType string,
	rateProcessor RateProcessor,
) (transform.Params, error) {
	if len(args) != 1 {
		return emptyOp,
			fmt.Errorf("invalid number of args for %s: %d", opType, len(args))
	}

	duration, ok := args[0].(time.Duration)
	if !ok {
		return emptyOp,
			fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], opType)
	}

	return newBaseOp(duration, opType, rateProcessor)
}

// NewRateOp creates a new base temporal transform for rate functions.
func NewRateOp(args []interface{}, opType string) (transform.Params, error) {
	var (
		isRate, isCounter bool
		rateFn            = standardRateFunc
	)

	switch opType {
	case IRateType:
		isRate = true
		rateFn = irateFunc
	case IDeltaType:
		rateFn = irateFunc
	case RateType:
		isRate = true
		isCounter = true
	case IncreaseType:
		isCounter = true
	case DeltaType:
	default:
		return nil, fmt.Errorf("unknown rate type: %s", opType)
	}

	r := RateProcessor{
		IsRate:    isRate,
		IsCounter: isCounter,
		RateFn:    rateFn,
	}

	return NewRateOpWithProcessor(args, opType, r)
}

// RateFn is a function that calculates rate over the given set of datapoints.
type RateFn func(
	datapoints ts.Datapoints,
	isRate bool,
	isCounter bool,
	rangeStart xtime.UnixNano,
	rangeEnd xtime.UnixNano,
	duration time.Duration,
) float64

type rateNode struct {
	isRate, isCounter bool
	duration          time.Duration
	rateFn            RateFn
}

func (r *rateNode) process(datapoints ts.Datapoints, bounds iterationBounds) float64 {
	return r.rateFn(
		datapoints,
		r.isRate,
		r.isCounter,
		bounds.start,
		bounds.end,
		r.duration,
	)
}

func standardRateFunc(
	datapoints ts.Datapoints,
	isRate bool,
	isCounter bool,
	rangeStart xtime.UnixNano,
	rangeEnd xtime.UnixNano,
	timeWindow time.Duration,
) float64 {
	if len(datapoints) < 2 {
		return math.NaN()
	}

	var (
		counterCorrection   float64
		firstVal, lastValue float64
		firstIdx, lastIdx   int
		firstTS, lastTS     xtime.UnixNano
		foundFirst          bool
	)

	for i, dp := range datapoints {
		if math.IsNaN(dp.Value) {
			continue
		}

		if !foundFirst {
			firstVal = dp.Value
			firstTS = dp.Timestamp
			firstIdx = i
			foundFirst = true
		}

		if isCounter && dp.Value < lastValue {
			counterCorrection += lastValue
		}

		lastValue = dp.Value
		lastTS = dp.Timestamp
		lastIdx = i
	}

	if firstIdx == lastIdx {
		return math.NaN()
	}

	durationToStart := subSeconds(firstTS, rangeStart)
	durationToEnd := subSeconds(rangeEnd, lastTS)
	sampledInterval := subSeconds(lastTS, firstTS)
	averageDurationBetweenSamples := sampledInterval / float64(lastIdx-firstIdx)

	resultValue := lastValue - firstVal + counterCorrection
	if isCounter && resultValue > 0 && firstVal >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (firstVal / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}

	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}

	resultValue = resultValue * (extrapolateToInterval / sampledInterval)
	if isRate {
		resultValue /= timeWindow.Seconds()
	}

	return resultValue
}

func irateFunc(
	datapoints ts.Datapoints,
	isRate bool,
	_ bool, _ xtime.UnixNano, _ xtime.UnixNano, _ time.Duration,
) float64 {
	dpsLen := len(datapoints)
	if dpsLen < 2 {
		return math.NaN()
	}

	nonNanIdx := dpsLen - 1
	// find idx for last non-NaN value
	indexLast := findNonNanIdx(datapoints, nonNanIdx)
	// if indexLast is 0 then you only have one value and should return a NaN
	if indexLast < 1 {
		return math.NaN()
	}

	nonNanIdx = findNonNanIdx(datapoints, indexLast-1)
	if nonNanIdx == -1 {
		return math.NaN()
	}

	previousSample := datapoints[nonNanIdx]
	lastSample := datapoints[indexLast]

	var resultValue float64
	if isRate && lastSample.Value < previousSample.Value {
		// Counter reset.
		resultValue = lastSample.Value
	} else {
		resultValue = lastSample.Value - previousSample.Value
	}

	if isRate {
		sampledInterval := lastSample.Timestamp.Sub(previousSample.Timestamp)
		if sampledInterval == 0 {
			return math.NaN()
		}

		resultValue /= sampledInterval.Seconds()
	}

	return resultValue
}

// findNonNanIdx iterates over the values backwards until we find a non-NaN
// value, then returns its index.
func findNonNanIdx(dps ts.Datapoints, startingIdx int) int {
	for i := startingIdx; i >= 0; i-- {
		if !math.IsNaN(dps[i].Value) {
			return i
		}
	}

	return -1
}
