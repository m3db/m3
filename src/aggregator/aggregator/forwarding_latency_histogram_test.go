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

package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestForwardingLatencyHistogramsRecordDuration(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	bucketsFn := func(key ForwardingLatencyBucketKey, numLatencyBuckets int) tally.Buckets {
		bucketWidth := 2 * key.Resolution / time.Duration(numLatencyBuckets)
		return tally.MustMakeLinearDurationBuckets(0, bucketWidth, numLatencyBuckets)
	}
	histograms := NewForwardingLatencyHistograms(s, bucketsFn)
	histograms.RecordDuration(10*time.Second, 1, 2*time.Second)
	histograms.RecordDuration(10*time.Second, 2, 2*time.Second)
	histograms.RecordDuration(time.Minute, 2, 20*time.Second)
	histograms.RecordDuration(10*time.Second, 1, 5*time.Second)
	histograms.RecordDuration(10*time.Second, 1, 2*time.Second)
	snapshot := s.Snapshot()
	histogramSnapshots := snapshot.Histograms()

	require.Equal(t, 3, len(histogramSnapshots))
	for _, input := range []struct {
		id                string
		expectedName      string
		expectedDurations map[time.Duration]int64
	}{
		{
			id:           "testScope.forwarding-latency+bucket-version=5,num-forwarded-times=1,resolution=10s",
			expectedName: "testScope.forwarding-latency",
			expectedDurations: map[time.Duration]int64{
				2 * time.Second: 2,
				5 * time.Second: 1,
			},
		},
		{
			id:           "testScope.forwarding-latency+bucket-version=5,num-forwarded-times=2,resolution=10s",
			expectedName: "testScope.forwarding-latency",
			expectedDurations: map[time.Duration]int64{
				2 * time.Second: 1,
			},
		},
		{
			id:           "testScope.forwarding-latency+bucket-version=5,num-forwarded-times=2,resolution=1m0s",
			expectedName: "testScope.forwarding-latency",
			expectedDurations: map[time.Duration]int64{
				21 * time.Second: 1,
			},
		},
	} {
		h, exists := histogramSnapshots[input.id]
		require.True(t, exists)
		require.Equal(t, input.expectedName, h.Name())
		actualDurations := h.Durations()
		for k, v := range input.expectedDurations {
			actual, exists := actualDurations[k]
			require.True(t, exists)
			require.Equal(t, v, actual)
		}
	}
}
