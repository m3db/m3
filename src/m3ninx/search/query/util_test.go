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

package query

import (
	"strings"
	"testing"

	"github.com/m3db/m3/src/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	query := search.NewMockQuery(mockCtrl)
	query.EXPECT().String().AnyTimes().Return("query")

	tests := []struct {
		name             string
		input            []search.Query
		expected         string
		expectedNegation string
	}{
		{
			name:             "0 queries",
			input:            nil,
			expected:         "",
			expectedNegation: "",
		},
		{
			name:             "1 query",
			input:            []search.Query{query},
			expected:         "query",
			expectedNegation: "negation(query)",
		},
		{
			name:             "multiple queries",
			input:            []search.Query{query, query, query},
			expected:         "query, query, query",
			expectedNegation: "negation(query), negation(query), negation(query)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var j, jn strings.Builder
			join(&j, test.input)
			joinNegation(&jn, test.input)
			require.Equal(t, test.expected, j.String())
			require.Equal(t, test.expectedNegation, jn.String())
		})
	}
}
