// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The abovale copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package parser

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElectionStateJSONMarshal(t *testing.T) {
	for _, input := range []struct {
		val Value
		str string
	}{
		{val: 1231243.123123, str: `"1.231243123123e+06"`},
		{val: 0.000000001, str: `"1e-09"`},
		{val: Value(math.NaN()), str: `"NaN"`},
		{val: Value(math.Inf(1)), str: `"+Inf"`},
		{val: Value(math.Inf(-1)), str: `"-Inf"`},
	} {
		b, err := json.Marshal(input.val)
		require.NoError(t, err)
		assert.Equal(t, input.str, string(b))

		var val Value
		json.Unmarshal([]byte(input.str), &val)
		if math.IsNaN(float64(input.val)) {
			assert.True(t, math.IsNaN(float64(val)))
		} else if math.IsInf(float64(input.val), 1) {
			assert.True(t, math.IsInf(float64(val), 1))
		} else if math.IsInf(float64(input.val), -1) {
			assert.True(t, math.IsInf(float64(val), -1))
		} else {
			assert.Equal(t, input.val, val)
		}
	}
}
