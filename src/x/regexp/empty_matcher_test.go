// Copyright (c) 2023  Uber Technologies, Inc.
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

package regexp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchesEmptyValue(t *testing.T) {
	tests := []struct {
		given string
		want  bool
	}{
		{given: "", want: true},
		{given: "x", want: false},
		{given: ".*", want: true},
		{given: ".+", want: false},
		{given: "x*", want: true},
		{given: "x+", want: false},
		{given: "|", want: true},
		{given: "\\|", want: false},
		{given: "[|]", want: false},
		{given: "(|)", want: true},
		{given: "a|b", want: false},
		{given: "a||b", want: true},
		{given: "a|", want: true},
		{given: "|a", want: true},
		{given: "|a|", want: true},
		{given: "a|b|c", want: false},
		{given: "||a|||b||||c||||", want: true},
		{given: "()", want: true},
		{given: "()*", want: true},
		{given: "()+", want: true},
		{given: "(ab)", want: false},
		{given: "(ab)+", want: false},
		{given: "(a|b)", want: false},
		{given: "(a||b)", want: true},
		{given: "(a|)", want: true},
		{given: "(|a)", want: true},
		{given: "(\\|a)", want: false},
		{given: "([|])", want: false},
		{given: "([|]a)", want: false},
		{given: "([|a])", want: false},
		{given: ".*|a", want: true},
		{given: ".+|a", want: false},
		{given: ".*|.+", want: true},
		{given: ".*|.*", want: true},
		{given: ".+|.+", want: false},
		{given: "a(|)", want: false},
		{given: "a(|)b", want: false},
		{given: "(|)(|)", want: true},
		{given: "(|)a(|)", want: false},
		{given: "(|).*(|)", want: true},
		{given: "(|).+(|)", want: false},
		{given: "\\d*", want: true},
		{given: "\\d+", want: false},
		{given: "(\\d*|a)", want: true},
		{given: "(a|\\d*)", want: true},
		{given: "(|\\d*)", want: true},
		{given: "a(\\d*)", want: false},
		{given: "(\\d*)a", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.given, func(t *testing.T) {
			res, err := MatchesEmptyValue([]byte(tt.given))
			require.NoError(t, err)
			require.Equal(t, tt.want, res)
		})
	}
}
