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

type tsRegistry struct {
	currentIdx        int
	numPointsPerDatum int
	tsGenMap          map[int]TSGenFn
}

func (reg *tsRegistry) Size() int {
	return len(reg.tsGenMap)
}

func (reg *tsRegistry) Get(i int) SyntheticTimeSeries {
	sz := reg.Size()
	idx := i % sz
	if idx < 0 {
		idx = idx + sz
	}
	datum, err := NewSyntheticTimeSeris(idx, reg.numPointsPerDatum, reg.tsGenMap[idx])
	if err != nil {
		panic(err)
	}
	return datum
}

// NewDefaultRegistry returns a Registry with default timeseries generators
func NewDefaultRegistry(numPointsPerDatum int) Registry {
	reg := &tsRegistry{
		numPointsPerDatum: numPointsPerDatum,
		tsGenMap:          make(map[int]TSGenFn),
	}
	reg.init()
	return reg
}

func (reg *tsRegistry) init() {
	// identity datum
	reg.addGenFn(func(i int) float64 {
		return float64(i)
	})

	// square datum
	reg.addGenFn(func(i int) float64 {
		return float64(i * i)
	})

	// TODO(prateek): make this bigger
}

func (reg *tsRegistry) addGenFn(f TSGenFn) {
	idx := reg.currentIdx
	reg.tsGenMap[idx] = f
	reg.currentIdx++
}
