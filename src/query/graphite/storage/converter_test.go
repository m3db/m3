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

package storage

import (
	"testing"

	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlob(t *testing.T) {
	noGlob := "some_sort of-string,with~no=globbing"
	expected := []byte(noGlob)
	actual := glob(noGlob)
	require.Equal(t, actual, expected)

	globbed := "foo*bar"
	expected = []byte("foo.*bar")
	actual = glob(globbed)
	require.Equal(t, actual, expected)

	globAndRegex := "foo*bar[rz]*(qux|quail)"
	expected = []byte("foo.*bar[rz].*(qux|quail)")
	actual = glob(globAndRegex)
	require.Equal(t, actual, expected)
}

func TestConvertMetricPartToMatcher(t *testing.T) {
	for i := 0; i < 100; i++ {
		globAndRegex := "foo*bar[rz]*(qux|quail)"
		expected := models.Matcher{
			Type:  models.MatchRegexp,
			Name:  graphite.TagName(i),
			Value: []byte("foo.*bar[rz].*(qux|quail)"),
		}

		actual := convertMetricPartToMatcher(i, globAndRegex)
		assert.Equal(t, expected, actual)
	}
}

func TestMatcherTerminator(t *testing.T) {
	for i := 0; i < 100; i++ {
		expected := models.Matcher{
			Type:  models.MatchNotRegexp,
			Name:  graphite.TagName(i),
			Value: []byte(".*"),
		}

		actual := matcherTerminator(i)
		assert.Equal(t, expected, actual)
	}
}
