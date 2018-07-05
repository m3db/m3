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

package test

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// EqualsWithNans helps compare float slices which have NaNs in them
func EqualsWithNans(t *testing.T, expected interface{}, actual interface{}) {
	debugMsg := fmt.Sprintf("expected: %v, actual: %v", expected, actual)
	switch v := expected.(type) {
	case [][]float64:
		actualV, ok := actual.([][]float64)
		if !ok {
			require.Fail(t, "actual should be of type [][]float64, found: %T", actual)
		}

		for i, vals := range v {
			equalsWithNans(t, vals, actualV[i], debugMsg)
		}

	case []float64:
		actualV, ok := actual.([]float64)
		if !ok {
			require.Fail(t, "actual should be of type []float64, found: %T", actual)
		}
		equalsWithNans(t, v, actualV, debugMsg)

	default:
		require.Fail(t, "unknown type: %T", v)
	}
}

func equalsWithNans(t *testing.T, expected []float64, actual []float64, debugMsg string) {
	require.Equal(t, len(expected), len(actual))
	for i, v := range expected {
		if math.IsNaN(v) {
			require.True(t, math.IsNaN(actual[i]), debugMsg)
		} else {
			require.Equal(t, v, actual[i], debugMsg)
		}
	}
}
