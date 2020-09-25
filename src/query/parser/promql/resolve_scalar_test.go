// Copyright (c) 2020 Uber Technologies, Inc.
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

package promql

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/require"
)

var scalarResolverTests = []struct {
	funcString string
	expected   float64
}{
	{"7+2-1", 8},
	{"(9+scalar(vector(-10)))", -1},
	{"(9+scalar(some_fetch))", math.NaN()},
	{"scalar(9+vector(4)) / 2", 6.5},
	{
		`scalar(
			scalar(
				scalar(
					vector( 20 - 4 ) ^ 0.5 - 2
				) - vector( 2 )
			) + vector(2)
		) * 9`,
		18,
	},
	{
		`5 - scalar(
			scalar(
				scalar(
					vector( 20 - 4 ) ^ vector(0.5) - vector(2)
				) - vector( 2 )
			) + vector(2)
		)`,
		3,
	},
	{"scalar(vector(1) + vector(2))", 3},
	{"scalar(vector(1) + scalar(vector(1) + vector(2)))", 4},
	{"scalar(vector(1) + scalar(vector(1) + scalar(vector(1) + vector(2))))", 5},
	{"(scalar(9+vector(4)) * 4 - 9+scalar(vector(3)))", 46},
	{"scalar(vector(1) + scalar(some_fetch == 1))", math.NaN()},
	{"scalar(vector(1) + scalar(1 == some_fetch))", math.NaN()},
	{"scalar(1 +vector(2 != bool 1))", 2},
	{"scalar(1 +vector(1 != bool 1))", 1},
	{"1 >= bool 1", 1},
	{"1 >= bool 2", 0},
}

func TestScalarResolver(t *testing.T) {
	for _, tt := range scalarResolverTests {
		t.Run(tt.funcString, func(t *testing.T) {
			parsed, err := Parse(tt.funcString, time.Second,
				models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			expr := parsed.(*promParser).expr
			actual, err := resolveScalarArgument(expr)

			require.False(t, actual.HasTimeValues)
			require.NoError(t, err)
			test.EqualsWithNans(t, tt.expected, actual.Scalar)
		})
	}
}

var timeScalarResolverTests = []struct {
	funcString string
	expected   []float64
}{
	{"7+2-1+time()", []float64{9, 10, 11, 12}},
	{"(9+time()+scalar(vector(-10)))", []float64{0, 1, 2, 3}},
	{"time()+time()*2+time()*3", []float64{6, 12, 18, 24}},
	{"(time()+time()*2+time())*3", []float64{12, 24, 36, 48}},
	{"time()*2000-(time()*1000)", []float64{1000, 2000, 3000, 4000}},
	{"time()*(2000-time()*1000)", []float64{1000, 0, -3000, -8000}},
	{"time() * 2000 -(time() * 1000)", []float64{1000, 2000, 3000, 4000}},
	{"scalar(vector(time()))", []float64{1, 2, 3, 4}},
	{"scalar(1+vector(time() != bool 1))", []float64{1, 2, 2, 2}},
	{"(time()+scalar(some_fetch))", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}},
	{"scalar(9+vector(time())) / 2", []float64{5, 5.5, 6, 6.5}},
	{
		`scalar(
			scalar(
				scalar(
					time() + vector(-time()) ^ 2 
				) - vector(+2)
			) + vector(time())
		)`,
		[]float64{1, 6, 13, 22},
	},
}

func TestScalarResolverWithTime(t *testing.T) {
	for _, tt := range timeScalarResolverTests {
		t.Run(tt.funcString, func(t *testing.T) {
			timeFn := func() []float64 {
				return []float64{1, 2, 3, 4}
			}

			parsed, err := Parse(tt.funcString, time.Second,
				models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			expr := parsed.(*promParser).expr
			actual, err := resolveScalarArgument(expr)

			require.NoError(t, err, tt.funcString)
			require.True(t, actual.HasTimeValues)
			applied := actual.TimeValueFn(timeFn)
			test.EqualsWithNans(t, tt.expected, applied)
		})
	}
}
