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

package ingestcarbon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
)

func TestCopyAndRewrite(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		cfg      *config.CarbonIngesterRewriteConfiguration
	}{
		{
			name:     "bad but no rewrite",
			input:    "foo$$.bar%%.baz@@",
			expected: "foo$$.bar%%.baz@@",
			cfg:      nil,
		},
		{
			name:     "bad but no rewrite cleanup",
			input:    "foo$$.bar%%.baz@@",
			expected: "foo$$.bar%%.baz@@",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: false,
			},
		},
		{
			name:     "good with rewrite cleanup",
			input:    "foo.bar.baz",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "bad with rewrite cleanup",
			input:    "foo$$.bar%%.baz@@",
			expected: "foo_.bar_.baz_",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "collapse two dots with rewrite cleanup",
			input:    "foo..bar.baz",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "collapse three and two dots with rewrite cleanup",
			input:    "foo...bar..baz",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "remove leading dot with rewrite cleanup",
			input:    ".foo.bar.baz",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "remove multiple leading dots with rewrite cleanup",
			input:    "..foo.bar.baz",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "remove trailing dot with rewrite cleanup",
			input:    "foo.bar.baz.",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
		{
			name:     "remove multiple trailing dots with rewrite cleanup",
			input:    "foo.bar.baz..",
			expected: "foo.bar.baz",
			cfg: &config.CarbonIngesterRewriteConfiguration{
				Cleanup: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := copyAndRewrite(nil, []byte(test.input), test.cfg)
			require.Equal(t, test.expected, string(actual))
		})
	}
}
