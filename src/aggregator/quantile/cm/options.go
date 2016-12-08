// Copyright (c) 2016 Uber Technologies, Inc.
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

package cm

import (
	"errors"
	"fmt"
)

const (
	defaultEps  = 1e-3
	minEps      = 0.0
	maxEps      = 0.5
	minQuantile = 0.0
	maxQuantile = 1.0
)

var (
	defaultQuantiles    = []float64{0.5, 0.95, 0.99}
	errInvalidEps       = fmt.Errorf("epsilon value must be between %f and %f", minEps, maxEps)
	errInvalidQuantiles = fmt.Errorf("quantiles must be nonempty and between %f and %f", minQuantile, maxQuantile)
	errNoSamplePool     = errors.New("no sample pool set")
	errNoFloatsPool     = errors.New("no floats pool set")
)

type options struct {
	eps        float64
	quantiles  []float64
	samplePool SamplePool
	floatsPool FloatsPool
}

// NewOptions creates a new options
func NewOptions() Options {
	samplePool := NewSamplePool(nil)
	samplePool.Init()

	floatsPool := NewFloatsPool(nil)
	floatsPool.Init()

	return options{
		eps:        defaultEps,
		quantiles:  defaultQuantiles,
		samplePool: samplePool,
		floatsPool: floatsPool,
	}
}

func (o options) SetEps(value float64) Options {
	o.eps = value
	return o
}

func (o options) Eps() float64 {
	return o.eps
}

func (o options) SetQuantiles(value []float64) Options {
	o.quantiles = value
	return o
}

func (o options) Quantiles() []float64 {
	return o.quantiles
}

func (o options) SetSamplePool(value SamplePool) Options {
	o.samplePool = value
	return o
}

func (o options) SamplePool() SamplePool {
	return o.samplePool
}

func (o options) SetFloatsPool(value FloatsPool) Options {
	o.floatsPool = value
	return o
}

func (o options) FloatsPool() FloatsPool {
	return o.floatsPool
}

func (o options) Validate() error {
	if o.eps <= minEps || o.eps >= maxEps {
		return errInvalidEps
	}
	if len(o.quantiles) == 0 {
		return errInvalidQuantiles
	}
	if o.samplePool == nil {
		return errNoSamplePool
	}
	if o.floatsPool == nil {
		return errNoFloatsPool
	}
	for _, quantile := range o.quantiles {
		if quantile <= minQuantile || quantile >= maxQuantile {
			return errInvalidQuantiles
		}
	}
	return nil
}
