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

package linear

import (
	"fmt"
	"math"

	"github.com/m3db/m3db/src/coordinator/executor/transform"
)

// RoundType rounds each value in the timeseries to the nearest integer.
// Ties are resolved by rounding up.
const RoundType = "round"

type roundOp struct {
	toNearest float64
}

// NewRoundOp creates a new round op based on the type and arguments
func NewRoundOp(args []interface{}) (BaseOp, error) {
	if len(args) > 1 {
		return emptyOp, fmt.Errorf("invalid number of args for round: %d", len(args))
	}

	var (
		toNearest = 1.0
		ok        bool
	)
	if len(args) > 0 {
		toNearest, ok = args[0].(float64)
		if !ok {
			return emptyOp, fmt.Errorf("unable to cast to toNearest argument: %v", args[0])
		}
	}

	spec := roundOp{
		toNearest: toNearest,
	}

	return BaseOp{
		operatorType: RoundType,
		processorFn:  makeRoundProcessor(spec),
	}, nil

}

func makeRoundProcessor(spec roundOp) makeProcessor {
	roundOp := spec
	return func(op BaseOp, controller *transform.Controller) Processor {
		return &roundNode{op: roundOp, controller: controller, toNearest: roundOp.toNearest}
	}
}

type roundNode struct {
	op         roundOp
	toNearest  float64
	controller *transform.Controller
}

func (r *roundNode) Process(values []float64) []float64 {
	// Invert as it seems to cause fewer floating point accuracy issues.
	toNearestInverse := 1.0 / r.toNearest

	for i := range values {
		v := math.Floor(values[i]*toNearestInverse+0.5) / toNearestInverse
		values[i] = v
	}
	return values
}
