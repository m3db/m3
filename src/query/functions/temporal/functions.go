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
)

const (
	// ResetsType returns the number of counter resets within the provided time
	// range as a time series. Any decrease in the value between two consecutive
	// datapoints is interpreted as a counter reset.
	// ResetsTemporalType should only be used with counters.
	ResetsType = "resets"

	// ChangesType returns the number of times a value changes within the
	// provided time range for a given time series.
	ChangesType = "changes"
)

type comparisonFunc func(a, b float64) bool

type functionProcessor struct {
	compFunc comparisonFunc
}

func (f functionProcessor) initialize(
	_ time.Duration,
	_ transform.Options,
) processor {
	return &functionNode{ 
		comparisonFunc: f.compFunc,
	}
}

// NewFunctionOp creates a new base temporal transform for functions
func NewFunctionOp(args []interface{}, optype string) (transform.Params, error) {
	var compFunc comparisonFunc

	switch optype {
	case ResetsType:
		compFunc = func(a, b float64) bool { return a < b }
	case ChangesType:
		compFunc = func(a, b float64) bool { return a != b }
	default:
		return nil, fmt.Errorf("unknown function type: %s", optype)
	}

	duration, ok := args[0].(time.Duration)
	if !ok {
		return emptyOp, fmt.
			Errorf("unable to cast to scalar argument: %v for %s", args[0], optype)
	}

	f := functionProcessor{
		compFunc: compFunc,
	}

	return newBaseOp(duration, optype, f)
}

type functionNode struct { 
	comparisonFunc comparisonFunc
}

func (f *functionNode) process(datapoints ts.Datapoints, _ iterationBounds) float64 {
	if len(datapoints) == 0 {
		return math.NaN()
	}

	allNaNs := true
	result := 0.0
	prev := datapoints[0].Value

	for _, curr := range datapoints[1:] {
		if math.IsNaN(curr.Value) {
			continue
		}

		allNaNs = false
		if !math.IsNaN(prev) {
			if f.comparisonFunc(curr.Value, prev) {
				result++
			}
		}

		prev = curr.Value
	}

	if allNaNs {
		return math.NaN()
	}

	return result
}
