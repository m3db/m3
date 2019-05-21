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

package handler

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchOptionsBuilder(t *testing.T) {
	tests := []struct {
		name          string
		defaultLimit  int
		headers       map[string]string
		expectedLimit int
		expectedErr   bool
	}{
		{
			name:          "default limit with no headers",
			defaultLimit:  42,
			headers:       map[string]string{},
			expectedLimit: 42,
		},
		{
			name:         "limit with header",
			defaultLimit: 42,
			headers: map[string]string{
				LimitMaxSeriesHeader: "4242",
			},
			expectedLimit: 4242,
		},
		{
			name:         "bad header",
			defaultLimit: 42,
			headers: map[string]string{
				LimitMaxSeriesHeader: "not_a_number",
			},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewFetchOptionsBuilder(FetchOptionsBuilderOptions{
				Limit: test.defaultLimit,
			})

			req := httptest.NewRequest("GET", "/foo", nil)
			for k, v := range test.headers {
				req.Header.Add(k, v)
			}

			opts, err := builder.NewFetchOptions(req)

			if !test.expectedErr {
				require.NoError(t, err)
				require.Equal(t, test.expectedLimit, opts.Limit)
			} else {
				require.Error(t, err)
			}
		})
	}
}
