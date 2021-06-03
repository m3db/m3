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

package transformation

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPerSecond(t *testing.T) {
	inputs := []struct {
		prev        Datapoint
		curr        Datapoint
		expectedNaN bool
		expected    Datapoint
	}{
		{
			prev:     Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 25},
			curr:     Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 30},
			expected: Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 0.5},
		},
		{
			prev:     Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 25},
			curr:     Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 30},
			expected: Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 0.5},
		},
		{
			prev:        Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 25},
			curr:        Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 20},
			expectedNaN: true,
			expected:    emptyDatapoint,
		},
		{
			prev:        Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: math.NaN()},
			curr:        Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 20},
			expectedNaN: true,
			expected:    emptyDatapoint,
		},
		{
			prev:        Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 20},
			curr:        Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: math.NaN()},
			expectedNaN: true,
			expected:    emptyDatapoint,
		},
		{
			prev:        Datapoint{TimeNanos: time.Unix(1230, 0).UnixNano(), Value: 30},
			curr:        Datapoint{TimeNanos: time.Unix(1240, 0).UnixNano(), Value: 20},
			expectedNaN: true,
			expected:    emptyDatapoint,
		},
	}

	for _, input := range inputs {
		if input.expectedNaN {
			require.True(t, perSecond(input.prev, input.curr, FeatureFlags{}).IsEmpty())
		} else {
			require.Equal(t, input.expected, perSecond(input.prev, input.curr, FeatureFlags{}))
		}
	}
}
