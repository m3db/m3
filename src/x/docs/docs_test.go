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

package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPath(t *testing.T) {
	p := Path("foo")
	assert.Equal(t, "https://docs.m3db.io/foo", p)
}

func TestParseRepoPathURL(t *testing.T) {
	tests := []struct {
		url     string
		expPath string
		inRepo  bool
	}{
		{
			url: "https://wikipedia.com/foo/bar",
		},
		{
			url:     "https://github.com/m3db/m3/blob/master/docs/foo.md",
			expPath: "docs/foo.md",
			inRepo:  true,
		},
		{
			url: "https://github.com/foo/bar/blob/master/docs/baz.md",
		},
	}

	for _, test := range tests {
		path, inRepo := ParseRepoPathURL(test.url)
		assert.Equal(t, test.inRepo, inRepo)
		assert.Equal(t, test.expPath, path.RepoPath)
	}
}
