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

package promql

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/require"
)

var scalarResolverTests = []struct {
	funcString  string
	expected    float64
	expectedErr error
}{
	{"(9+scalar(vector(-10)))", -1, nil},
	{"(9+scalar(some_fetch))", math.NaN(), nil},
	{"scalar(9+vector(4)) / 2", 6.5, nil},
	{
		`scalar(
			scalar(
				scalar(
					vector( 20 - 4 ) ^ 0.5 - 2
				) - vector( 2 )
			) + vector(2)
		)`,
		2,
		nil,
	},
	{
		`scalar(
			scalar(
				scalar(
					vector( 20 - 4 ) ^ vector(0.5) - vector(2)
				) - vector( 2 )
			) + vector(2)
		)`,
		2,
		nil,
	},
	{"some_fetch", 0, errInvalidNestingFetch},
	// {"scalar(scalar(some_fetch))", 0, errInvalidNestingFetch},
	// {"scalar(scalar(5))", 0, errors.New("promql.resolveScalarArgument: invalid nesting 2")},
}

func TestScalarResolver(t *testing.T) {
	for _, tt := range scalarResolverTests {
		t.Run(tt.funcString, func(t *testing.T) {
			parsed, err := Parse(tt.funcString)
			require.NoError(t, err)
			expr := parsed.(*promParser).expr
			actual, err := resolveScalarArgument(expr)

			require.Equal(t, tt.expectedErr, err)
			test.EqualsWithNans(t, tt.expected, actual)
		})
	}
}
