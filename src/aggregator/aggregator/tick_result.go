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

package aggregator

import "time"

type tickResultForMetricCategory struct {
	activeElems    map[time.Duration]int
	activeEntries  int
	expiredEntries int
}

func (r *tickResultForMetricCategory) merge(
	other tickResultForMetricCategory,
) tickResultForMetricCategory {
	res := tickResultForMetricCategory{
		activeEntries:  r.activeEntries + other.activeEntries,
		expiredEntries: r.expiredEntries + other.expiredEntries,
	}
	if len(r.activeElems) == 0 {
		res.activeElems = other.activeElems
		return res
	}
	if len(other.activeElems) == 0 {
		res.activeElems = r.activeElems
		return res
	}
	for dur, val := range other.activeElems {
		r.activeElems[dur] += val
	}
	res.activeElems = r.activeElems
	return res
}

type tickResult struct {
	standard  tickResultForMetricCategory
	forwarded tickResultForMetricCategory
	timed     tickResultForMetricCategory
}

// merge merges two results. Both input results may become invalid after merge is called.
func (r *tickResult) merge(other tickResult) tickResult {
	return tickResult{
		standard:  r.standard.merge(other.standard),
		forwarded: r.forwarded.merge(other.forwarded),
		timed:     r.timed.merge(other.timed),
	}
}
