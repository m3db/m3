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

package tile

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func a(s string) ts.Annotation { return ts.Annotation(s) }

func annotationEqual(t *testing.T, expected string, actual ts.Annotation) {
	assert.Equal(t, expected, string(actual))
}

func annotationsEqual(t *testing.T, expected []string, actual []ts.Annotation) {
	require.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		annotationEqual(t, ex, actual[i])
	}
}

func TestSeriesFrameAnnotationsSingle(t *testing.T) {
	rec := newAnnotationRecorder()

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	rec.record(a("foo"))
	v, ok := rec.SingleValue()
	assert.True(t, ok)
	annotationEqual(t, "foo", v)

	rec.record(a("foo"))
	v, ok = rec.SingleValue()
	assert.True(t, ok)
	annotationEqual(t, "foo", v)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "foo"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)
}

func TestSeriesFrameAnnotationsMultiple(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("foo"))
	rec.record(a("bar"))

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "foo", "bar"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}

func TestSeriesFrameAnnotationsMultipleChanges(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("bar"))
	rec.record(a("baz"))

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "bar", "baz"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}
