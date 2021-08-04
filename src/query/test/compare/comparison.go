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

// Package compare provides comparison functions for testing.
package compare

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// EqualsWithNans helps compare float slices which have NaNs in them
func EqualsWithNans(t *testing.T, expected interface{}, actual interface{}) {
	EqualsWithNansWithDelta(t, expected, actual, 0)
}

// EqualsWithNansWithDelta helps compare float slices which have NaNs in them
// allowing a delta for float comparisons.
func EqualsWithNansWithDelta(t *testing.T,
	expected interface{}, actual interface{}, delta float64) {
	debugMsg := fmt.Sprintf("expected: %v, actual: %v", expected, actual)
	switch v := expected.(type) {
	case [][]float64:
		actualV, ok := actual.([][]float64)
		require.True(t, ok, "actual should be of type [][]float64, found: %T", actual)
		require.Equal(t, len(v), len(actualV),
			fmt.Sprintf("expected length: %v, actual length: %v\nfor expected: %v, actual: %v",
				len(v), len(actualV), expected, actual))
		for i, vals := range v {
			debugMsg = fmt.Sprintf("on index %d, expected: %v, actual: %v", i, vals, actualV[i])
			equalsWithNans(t, vals, actualV[i], delta, debugMsg)
		}

	case []float64:
		actualV, ok := actual.([]float64)
		require.True(t, ok, "actual should be of type []float64, found: %T", actual)
		require.Equal(t, len(v), len(actualV),
			fmt.Sprintf("expected length: %v, actual length: %v\nfor expected: %v, actual: %v",
				len(v), len(actualV), expected, actual))

		equalsWithNans(t, v, actualV, delta, debugMsg)

	case float64:
		actualV, ok := actual.(float64)
		require.True(t, ok, "actual should be of type float64, found: %T", actual)
		equalsWithDelta(t, v, actualV, delta, debugMsg)

	default:
		require.Fail(t, "unknown type: %T", v)
	}
}

func equalsWithNans(t *testing.T, expected []float64,
	actual []float64, delta float64, debugMsg string) {
	require.Equal(t, len(expected), len(actual))
	for i, v := range expected {
		if math.IsNaN(v) {
			require.True(t, math.IsNaN(actual[i]), debugMsg)
		} else {
			equalsWithDelta(t, v, actual[i], delta, debugMsg)
		}
	}
}

func equalsWithDelta(t *testing.T, expected, actual, delta float64, debugMsg string) {
	if math.IsNaN(expected) {
		require.True(t, math.IsNaN(actual), debugMsg)
		return
	}
	if math.IsInf(expected, 0) {
		require.Equal(t, expected, actual, debugMsg)
		return
	}
	if delta == 0 {
		require.Equal(t, expected, actual, debugMsg)
	} else {
		diff := math.Abs(expected - actual)
		require.True(t, delta > diff, debugMsg)
	}
}

type match struct {
	indices    []int
	seriesTags []models.Tag
	name       []byte
	values     []float64
}

type matches []match

func (m matches) Len() int      { return len(m) }
func (m matches) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m matches) Less(i, j int) bool {
	return bytes.Compare(
		models.NewTags(0, nil).AddTags(m[i].seriesTags).ID(),
		models.NewTags(0, nil).AddTags(m[j].seriesTags).ID(),
	) == -1
}

// CompareListsInOrder compares series meta / index pairs (order sensitive)
func CompareListsInOrder(t *testing.T, meta, exMeta []block.SeriesMeta, index, exIndex [][]int) {
	require.Equal(t, len(exIndex), len(exMeta))

	assert.Equal(t, exMeta, meta)
	assert.Equal(t, exIndex, index)
}

// CompareValuesInOrder compares series meta / value pairs (order sensitive)
func CompareValuesInOrder(t *testing.T, meta, exMeta []block.SeriesMeta, vals, exVals [][]float64) {
	require.Equal(t, len(exVals), len(exMeta))
	require.Equal(t, len(exVals), len(vals), "Vals is", meta, "ExVals is", exMeta)

	assert.Equal(t, exMeta, meta)

	for i := range exVals {
		EqualsWithNansWithDelta(t, exVals[i], vals[i], 0.00001)
	}
}

// CompareValues compares series meta / value pairs (order insensitive)
func CompareValues(t *testing.T, meta, exMeta []block.SeriesMeta, vals, exVals [][]float64) {
	require.Equal(t, len(exVals), len(exMeta))
	require.Equal(t, len(exMeta), len(meta), "Meta is", meta, "ExMeta is", exMeta)
	require.Equal(t, len(exVals), len(vals), "Vals is", meta, "ExVals is", exMeta)

	ex := make(matches, len(meta))
	actual := make(matches, len(meta))
	// build matchers
	for i := range meta {
		ex[i] = match{[]int{}, exMeta[i].Tags.Tags, exMeta[i].Name, exVals[i]}
		actual[i] = match{[]int{}, meta[i].Tags.Tags, exMeta[i].Name, vals[i]}
	}

	sort.Sort(ex)
	sort.Sort(actual)
	for i := range ex {
		assert.Equal(t, ex[i].name, actual[i].name)
		assert.Equal(t, ex[i].seriesTags, actual[i].seriesTags)
		EqualsWithNansWithDelta(t, ex[i].values, actual[i].values, 0.00001)
	}
}
