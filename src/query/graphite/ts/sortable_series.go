// Copyright (c) 2019 Uber Technologies, Inc.
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

package ts

import (
	"math"
	"sort"
)

// Direction signifies ascending or descending order
type Direction int

const (
	// Ascending order
	Ascending Direction = iota
	// Descending order
	Descending
)

type sortableSeries struct {
	series *Series
	value  float64
}

type sortableSeriesList []sortableSeries

func (s sortableSeriesList) Len() int { return len(s) }

func (s sortableSeriesList) Less(i, j int) bool {
	if math.IsNaN(s[i].value) && !math.IsNaN(s[j].value) {
		return true
	}
	return s[i].value < s[j].value
}

func (s sortableSeriesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// SortSeries applies a given SeriesReducer to each series in the input
// list and sorts based on the assigned value
func SortSeries(input SeriesList, sr SeriesReducer, dir Direction) (SeriesList, error) {
	var (
		in           = input.Values
		sortableList = make(sortableSeriesList, 0, len(in))
		results      = make([]*Series, 0, len(in))
	)

	for _, series := range in {
		results = append(results, series)
	}

	if !input.SortApplied {
		// If no sort was applied to these series then sort by name
		// before we do the sort.Stable so that if two series are equal
		// (for instance if comparing by min and they all share same min)
		// that you get the same minimum series each time for same set of
		// series.
		sort.Stable(SeriesByName(results))
	}

	for _, series := range results {
		sortableList = append(sortableList, sortableSeries{series: series, value: sr(series)})
	}

	// Use sort.Stable for deterministic output.
	if dir == Ascending {
		sort.Stable(sortableList)
	} else {
		sort.Stable(sort.Reverse(sortableList))
	}

	// Set results in the sort order of the sortable series list.
	results = results[:0]
	for _, sortable := range sortableList {
		results = append(results, sortable.series)
	}

	// Preserve metadata and ensure that sort applied is set to true.
	input.Values = results
	input.SortApplied = true
	return input, nil
}
