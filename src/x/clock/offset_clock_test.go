// Copyright (c) 2021 Uber Technologies, Inc.
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

package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOffsetClockNow(t *testing.T) {
	OneYearFromNow := time.Now().Add(365 * 24 * time.Hour).Truncate(time.Nanosecond)

	tests := []struct {
		name       string
		offsetTime time.Time
		expected   []time.Time
	}{
		{
			name:       "past",
			offsetTime: time.Unix(1614245284, 0),
			expected: []time.Time{
				time.Unix(1614245285, 0),
				time.Unix(1614245286, 0),
				time.Unix(1614245287, 0),
			},
		},
		{
			name:       "initial unix time",
			offsetTime: time.Unix(0, 0),
			expected: []time.Time{
				time.Unix(1, 0),
				time.Unix(2, 0),
				time.Unix(3, 0),
			},
		},
		{
			name:       "future",
			offsetTime: OneYearFromNow,
			expected: []time.Time{
				OneYearFromNow.Add(1 * time.Second),
				OneYearFromNow.Add(2 * time.Second),
				OneYearFromNow.Add(3 * time.Second),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sut := NewOffsetClock(tt.offsetTime, advanceByOneSec)
			for _, expected := range tt.expected {
				actual := sut.Now()
				assert.Equal(t, expected.UnixNano(), actual.UnixNano())
			}
		})
	}
}

var initialSeedTime = time.Now()

func advanceByOneSec() time.Time {
	initialSeedTime = initialSeedTime.Add(1 * time.Second)
	return initialSeedTime
}
