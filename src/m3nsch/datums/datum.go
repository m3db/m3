// Copyright (c) 2017 Uber Technologies, Inc.
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

package datums

import "fmt"

var (
	errNumPointsNegative = fmt.Errorf("numPoints must be positive")
)

type synTS struct {
	id   int
	data []float64

	lastIdx int
}

func (ld *synTS) ID() int {
	return ld.id
}

func (ld *synTS) Size() int {
	return len(ld.data)
}

func (ld *synTS) Data() []float64 {
	return ld.data
}

func (ld *synTS) Get(idx int) float64 {
	idx = idx % len(ld.data)
	if idx < 0 {
		idx += len(ld.data)
	}
	return ld.data[idx]
}

func (ld *synTS) Next() float64 {
	idx := (ld.lastIdx + 1) % len(ld.data)
	if idx < 0 {
		idx += len(ld.data)
	}
	ld.lastIdx = idx
	return ld.data[idx]
}

// NewSyntheticTimeSeris generates a new SyntheticTimeSeris using the provided parameters.
func NewSyntheticTimeSeris(id int, numPoints int, fn TSGenFn) (SyntheticTimeSeries, error) {
	if numPoints < 0 {
		return nil, errNumPointsNegative
	}
	data := make([]float64, numPoints)
	for i := 0; i < numPoints; i++ {
		data[i] = fn(i)
	}
	return &synTS{
		id:      id,
		data:    data,
		lastIdx: -1,
	}, nil
}
