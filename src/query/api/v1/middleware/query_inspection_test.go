// Copyright (c) 2021 Uber Technologies, Inc.
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

package middleware

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestRetrieveQueryRange(t *testing.T) {
	tests := []struct {
		query string
		ex    time.Duration
	}{
		{
			query: "up",
			ex:    time.Minute * 5,
		},
		{
			query: "up offset 3m",
			ex:    time.Minute * 8,
		},
		{
			query: "rate(up[3m])",
			ex:    time.Minute * 3,
		},
		{
			query: "rate(up[10m])",
			ex:    time.Minute * 10,
		},
		{
			query: "rate(up[1m]) + rate(down[9m])",
			ex:    time.Minute * 9,
		},
		{
			query: "min_over_time(rate(up[1m] offset 1m)[11m:9m])",
			ex:    time.Minute * 13, // 11m subquery range + 1m offset + 1m rate range
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.ex, retrieveQueryRange(expr, 0))
		})
	}
}
