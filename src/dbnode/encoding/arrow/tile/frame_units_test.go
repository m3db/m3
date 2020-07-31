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

	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesFrameUnitsSingle(t *testing.T) {
	rec := newUnitRecorder()

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	rec.record(xtime.Second)
	v, ok := rec.SingleValue()
	assert.True(t, ok)
	assert.Equal(t, xtime.Second, v)

	rec.record(xtime.Second)
	v, ok = rec.SingleValue()
	assert.True(t, ok)
	assert.Equal(t, xtime.Second, v)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Second}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)
}

func TestSeriesFrameUnitsMultiple(t *testing.T) {
	rec := newUnitRecorder()
	rec.record(xtime.Second)
	rec.record(xtime.Second)
	rec.record(xtime.Day)

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Second, xtime.Day}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}

func TestSeriesFrameUnitsMultipleChanges(t *testing.T) {
	rec := newUnitRecorder()
	rec.record(xtime.Second)
	rec.record(xtime.Day)
	rec.record(xtime.Nanosecond)

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Day, xtime.Nanosecond}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}
