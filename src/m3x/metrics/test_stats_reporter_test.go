// Copyright (c) 2016 Uber Technologies, Inc.
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

package xmetrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTestStatsReporter(t *testing.T) {
	tags := map[string]string{"my-key": "my-val"}
	s := NewTestStatsReporter()
	s.IncCounter("foo", nil, 100)
	s.IncCounter("foo", nil, 25)
	s.IncCounter("foo", tags, 176)
	s.UpdateGauge("foo", tags, 78)
	s.RecordTimer("foo", tags, time.Millisecond*45)

	assert.EqualValues(t, 125, s.Counter("foo", nil))
	assert.EqualValues(t, 176, s.Counter("foo", tags))
	assert.EqualValues(t, 0, s.Gauge("foo", nil)) // No untagged gauge
	assert.EqualValues(t, 78, s.Gauge("foo", tags))
	assert.Nil(t, s.Timer("foo", nil)) // No untagged timer
	assert.EqualValues(t, 45, s.Timer("foo", tags).Quantile(.5))
}
