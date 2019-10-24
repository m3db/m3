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

package promql

import (
	"bytes"
	"testing"

	"github.com/m3db/m3/src/query/models"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelMatchesToModelMatcher(t *testing.T) {
	opts := models.NewTagOptions()

	labels := []*labels.Matcher{
		&labels.Matcher{
			Type: labels.MatchEqual,
			Name: "foo",
		},
		&labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  "foo",
			Value: "bar",
		},
		&labels.Matcher{
			Type: labels.MatchNotEqual,
			Name: "foo",
		},
		&labels.Matcher{
			Type:  labels.MatchNotEqual,
			Name:  "foo",
			Value: "bar",
		},
		&labels.Matcher{
			Type:  labels.MatchRegexp,
			Name:  "foo",
			Value: ".*",
		},
		&labels.Matcher{
			Type:  labels.MatchNotRegexp,
			Name:  "foo",
			Value: ".*",
		},
	}

	matchers, err := LabelMatchersToModelMatcher(labels, opts)
	assert.NoError(t, err)

	expected := models.Matchers{
		models.Matcher{
			Type:  models.MatchNotField,
			Name:  []byte("foo"),
			Value: []byte{},
		},
		models.Matcher{
			Type:  models.MatchEqual,
			Name:  []byte("foo"),
			Value: []byte("bar"),
		},
		models.Matcher{
			Type:  models.MatchField,
			Name:  []byte("foo"),
			Value: []byte{},
		},
		models.Matcher{
			Type:  models.MatchNotEqual,
			Name:  []byte("foo"),
			Value: []byte("bar"),
		},
		models.Matcher{
			Type:  models.MatchRegexp,
			Name:  []byte("foo"),
			Value: []byte(".*"),
		},
		models.Matcher{
			Type:  models.MatchNotRegexp,
			Name:  []byte("foo"),
			Value: []byte(".*"),
		},
	}

	require.Equal(t, len(expected), len(matchers))
	equalish := func(a, b models.Matcher) bool {
		return bytes.Equal(a.Name, b.Name) &&
			bytes.Equal(a.Value, b.Value) &&
			a.Type == b.Type
	}

	for i, ex := range expected {
		assert.True(t, equalish(ex, matchers[i]))
	}
}
