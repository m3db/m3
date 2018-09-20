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

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/ts"
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

type rateProcessor struct {
	isRate, isCounter bool
	rateFn            rateFn
}

func (r rateProcessor) Init(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	var (
		isRate, isCounter bool
		rateFn            = standardRateFunc
	)

	switch op.operatorType {
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
	}

	return &rateNode{
		op:         op,
		controller: controller,
		timeSpec:   opts.TimeSpec,
		isRate:     isRate,
		isCounter:  isCounter,
		rateFn:     rateFn,
	}
}

// NewRateOp creates a new base temporal transform for rate functions
func NewRateOp(args []interface{}, optype string) (transform.Params, error) {
	if optype == IRateType || optype == IDeltaType || optype == RateType ||
		optype == IncreaseType || optype == DeltaType {
		r := rateProcessor{}
		return newBaseOp(args, optype, r)
	}

	return nil, fmt.Errorf("unknown rate type: %s", optype)
}

type rateFn func([]float64, bool, bool, transform.TimeSpec) float64

type rateNode struct {
	op                baseOp
	controller        *transform.Controller
	timeSpec          transform.TimeSpec
	isRate, isCounter bool
	rateFn            rateFn
}

func (r *rateNode) Process(datapoints ts.Datapoints) float64 {
	return r.rateFn(datapoints, r.isRate, r.isCounter, r.timeSpec)
}

func standardRateFunc(datapoints ts.Datapoints, isRate, isCounter bool, timeSpec transform.TimeSpec) float64 {
	var (
		rangeStart = float64(timeSpec.Start.Unix()) - timeSpec.Step.Seconds()
		rangeEnd   = float64(timeSpec.End.Unix()) - timeSpec.Step.Seconds()

		counterCorrection   float64
		firstVal, lastValue float64
		firstIdx, lastIdx   int
		foundFirst          bool
	)

	if len(datapoints) < 2 {
		return math.NaN()
	}

	for i, dp := range datapoints {
		if math.IsNaN(dp.Value) {
			continue
		}

		if !foundFirst {
			firstVal = dp.Value
			firstIdx = i
			foundFirst = true
		}

		if isCounter && dp.Value < lastValue {
			counterCorrection += lastValue
		}

		lastValue = dp.Value
		lastIdx = i
	}

	if firstIdx == lastIdx {
		return math.NaN()
	}

	resultValue := lastValue - firstVal + counterCorrection

	// Duration between first/last samples and boundary of range.
	firstTS := float64(timeSpec.Start.Unix()) + (timeSpec.Step.Seconds() * float64(firstIdx))
	lastTS := float64(timeSpec.Start.Unix()) + (timeSpec.Step.Seconds() * float64(lastIdx))
	durationToStart := firstTS - rangeStart
	durationToEnd := rangeEnd - lastTS

	sampledInterval := lastTS - firstTS
	averageDurationBetweenSamples := sampledInterval / float64(lastIdx)

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
		resultValue = resultValue / (float64(timeSpec.End.Unix()) - float64(timeSpec.Start.Unix()))
	}

	return resultValue
}

func irateFunc(datapoints ts.Datapoints, isRate bool, _ bool, timeSpec transform.TimeSpec) float64 {
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
	if r.isRate && lastSample.Value < previousSample.Value {
		// Counter reset.
		resultValue = lastSample.Value
	} else {
		resultValue = lastSample.Value - previousSample.Value
	}

	if r.isRate {
		sampledInterval := lastSample.Timestamp.Sub(previousSample.Timestamp)
		if sampledInterval == 0 {
			return math.NaN()
		}

		resultValue /= sampledInterval.Seconds()
	}

	return resultValue
}

// findNonNanIdx iterates over the values backwards until we find a non-NaN value,
// then returns its index
func findNonNanIdx(dps ts.Datapoints, startingIdx int) int {
	for i := startingIdx; i >= 0; i-- {
		if !math.IsNaN(dps[i].Value) {
			return i
		}
	}

	return -1
}
