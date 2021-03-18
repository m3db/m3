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

package transformation

import (
	"math"
	"time"
)

// transformReset returns the provided datapoint and a zero datapoint one second later.
//
// This transform is useful for force resetting a counter value in Prometheus. When running the M3Aggregator in HA, both
// the follower and leader are computing aggregate counters, but they started counting at different times. If these
// counters are emitted as monotonic cumulative counters, during failover the counter decreases if the new leader
// started counting later. Prometheus assumes any decrease in a counter is due a counter reset, which leads to strange
// display results since the counter did not actually reset.
//
// This transform gets around this issue by explicitly not accumulating results, like Add, and force resets the counter
// with a zero value so PromQL properly graphs the delta as the rate value.
//
// This does have the downside of an extra 0 datapoint per resolution period. The storage cost is more than just the
// extra 0 value since the value is stored 1 second after the actual datapoint. This degrades the timestamp encoding
// since the timestamps are no longer at a fixed interval. In practice we see a 3x increase in storage for these
// aggregated counters.
//
// Currently only a single extra datapoint per aggregation is supported. If multiple transforms in an aggregation emit
// an additional datapoint, only the last one is used.
func transformReset() UnaryMultiOutputTransform {
	return UnaryMultiOutputTransformFn(func(dp Datapoint, resolution time.Duration) (Datapoint, Datapoint) {
		// Add the reset datapoint to be half the resolution period to ensure equal spacing between datapoints.
		// We take the max with 1 to ensure there's at least a 1 nanosecond gap.
		resetWindow := int64(math.Max(float64(resolution.Nanoseconds()/2), 1))

		return dp, Datapoint{Value: 0, TimeNanos: dp.TimeNanos + resetWindow*int64(time.Nanosecond)}
	})
}
