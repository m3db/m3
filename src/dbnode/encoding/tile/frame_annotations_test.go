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

func (a *annotationRecorder) assertValue(t *testing.T, expected string, idx int) {
	actual, err := a.Value(idx)
	require.NoError(t, err)
	assert.Equal(t, expected, string(actual), "index %d", idx)
}

func (a *annotationRecorder) assertError(t *testing.T, idx int) {
	_, err := a.Value(idx)
	assert.Error(t, err, "index %d", idx)
}

func TestSeriesFrameAnnotationsEmpty(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.assertError(t,-1)
	rec.assertError(t,0)
}

func TestSeriesFrameAnnotationsSingle(t *testing.T) {
	rec := newAnnotationRecorder()

	rec.record(a("foo"))
	rec.assertValue(t, "foo", 0)
	rec.assertError(t,1)

	rec.record(a("foo"))
	rec.assertValue(t, "foo", 1)
	rec.assertError(t,2)

	rec.reset()
	rec.assertError(t,0)
}

func TestSeriesFrameAnnotationsMultiple(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("foo"))
	rec.record(a("bar"))

	rec.assertValue(t, "foo", 0)
	rec.assertValue(t, "foo", 1)
	rec.assertValue(t, "bar", 2)
	rec.assertError(t, 3)

	rec.reset()
	rec.assertError(t,0)
}

func TestSeriesFrameAnnotationsMultipleChanges(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("bar"))
	rec.record(a("baz"))
	rec.record(a("foo"))

	rec.assertValue(t, "foo", 0)
	rec.assertValue(t, "bar", 1)
	rec.assertValue(t, "baz", 2)
	rec.assertValue(t, "foo", 3)
	rec.assertError(t, 4)

	rec.reset()
	rec.assertError(t,0)
}
