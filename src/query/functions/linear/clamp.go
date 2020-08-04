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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// ClampMinType ensures all values except NaNs are greater
	// than or equal to the provided argument.
	ClampMinType = "clamp_min"

	// ClampMaxType ensures all values except NaNs are lesser
	// than or equal to provided argument.
	ClampMaxType = "clamp_max"
)

type clampOp struct {
	opType string
	scalar float64
}

func parseClampArgs(args []interface{}) (float64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("invalid number of args for clamp: %d", len(args))
	}

	scalar, ok := args[0].(float64)
	if !ok {
		return 0, fmt.Errorf("unable to cast to scalar argument: %v", args[0])
	}

	return scalar, nil
}

func clampFn(max bool, roundTo float64) block.ValueTransform {
	if max {
		return func(v float64) float64 { return math.Min(v, roundTo) }
	}

	return func(v float64) float64 { return math.Max(v, roundTo) }
}

func removeName(meta []block.SeriesMeta) []block.SeriesMeta {
	for i, m := range meta {
		meta[i].Tags = m.Tags.WithoutName()
	}

	return meta
}

// NewClampOp creates a new clamp op based on the type and arguments
func NewClampOp(args []interface{}, opType string) (parser.Params, error) {
	isMax := opType == ClampMaxType
	if opType != ClampMinType && !isMax {
		return nil, fmt.Errorf("unknown clamp type: %s", opType)
	}

	clampTo, err := parseClampArgs(args)
	if err != nil {
		return nil, err
	}

	fn := clampFn(isMax, clampTo)
	lazyOpts := block.NewLazyOptions().
		SetValueTransform(fn).
		SetSeriesMetaTransform(removeName)
	return lazy.NewLazyOp(opType, lazyOpts)
}
