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

package downsample

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBufferForPastTimedMetric(t *testing.T) {
	limits := defaultBufferPastLimits
	tests := []struct {
		value    time.Duration
		expected time.Duration
	}{
		{value: -1 * time.Second, expected: 15 * time.Second},
		{value: 0, expected: 15 * time.Second},
		{value: 1 * time.Second, expected: 15 * time.Second},
		{value: 29 * time.Second, expected: 15 * time.Second},
		{value: 30 * time.Second, expected: 30 * time.Second},
		{value: 59 * time.Second, expected: 30 * time.Second},
		{value: 60 * time.Second, expected: time.Minute},
		{value: 2 * time.Minute, expected: 2 * time.Minute},
		{value: 59 * time.Minute, expected: 2 * time.Minute},
		{value: 61 * time.Minute, expected: 2 * time.Minute},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("test_value_%v", test.value), func(t *testing.T) {
			result := bufferForPastTimedMetric(limits, test.value)
			require.Equal(t, test.expected, result)
		})
	}
}
