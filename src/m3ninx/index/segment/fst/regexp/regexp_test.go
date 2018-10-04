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

// Adapted from: https://raw.githubusercontent.com/blevesearch/bleve/master/index/scorch/segment/regexp_test.go
package regexp

import (
	"fmt"
	"regexp/syntax"
	"testing"
)

func TestLiteralPrefix(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{"", ""},
		{"hello", "hello"},
		{"hello.?", "hello"},
		{"hello$", "hello"},
		{`[h][e][l][l][o].*world`, "hello"},
		{`[h-h][e-e][l-l][l-l][o-o].*world`, "hello"},
		{".*", ""},
		{"h.*", "h"},
		{"h.?", "h"},
		{"h[a-z]", "h"},
		{`h\s`, "h"},
		{`(hello)world`, ""},
		{`日本語`, "日本語"},
		{`日本語\w`, "日本語"},
		{`^hello`, ""},
		{`^`, ""},
		{`$`, ""},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test: %d, %+v", i, test), func(t *testing.T) {
			s, err := syntax.Parse(test.input, syntax.Perl)
			if err != nil {
				t.Fatalf("expected no syntax.Parse error, got: %v", err)
			}

			got := LiteralPrefix(s)
			if test.expected != got {
				t.Fatalf("got: %s", got)
			}
		})
	}
}
