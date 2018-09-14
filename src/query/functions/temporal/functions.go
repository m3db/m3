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
)

const (
	// ResetsTemporalType returns the number of counter resets within the provided time range as a time series.
	// Any decrease in the value between two consecutive datapoints is interpreted as a counter reset.
	// ResetsTemporalType should only be used with counters.
	ResetsTemporalType = "resets"

	// ChangesTemporalType returns the number of times a value changes within the provided time range for
	// a given time series.
	ChangesTemporalType = "changes"
)

var (
	temporalFuncs = map[string]temporalFunc{
		ResetsTemporalType:  resets,
		ChangesTemporalType: changes,
	}
)

// NewFunctionOp creates a new base temporal transform for functions
func NewFunctionOp(args []interface{}, optype string) (transform.Params, error) {
	if temporalFunc, ok := temporalFuncs[optype]; ok {
		return newBaseOp(args, optype, newFunctionNode, temporalFunc)
	}

	return nil, fmt.Errorf("unknown function type: %s", optype)
}

func newFunctionNode(op baseOp, controller *transform.Controller) Processor {
	return &functionNode{
		op:           op,
		controller:   controller,
		temporalFunc: op.temporalFunc,
	}
}

type functionNode struct {
	op           baseOp
	controller   *transform.Controller
	temporalFunc temporalFunc
}

func (f *functionNode) Process(values []float64) float64 {
	return f.temporalFunc(values)
}

func resets(vals []float64) float64 {
	resets := 0.0
	prev := vals[0]
	var curr float64

	for _, val := range vals[1:] {
		curr = val
		if !math.IsNaN(prev) {
			if !math.IsNaN(curr) {
				if curr < prev {
					resets++
				}
				prev = curr
				continue
			}
			continue
		}
		prev = curr
	}
	return resets
}

func changes(vals []float64) float64 {
	changes := 0.0
	prev := vals[0]
	var curr float64

	for _, val := range vals[1:] {
		curr = val
		if !math.IsNaN(prev) {
			if !math.IsNaN(curr) {
				if curr != prev {
					changes++
				}
				prev = curr
				continue
			}
			continue
		}
		prev = curr
	}
	return changes
}
