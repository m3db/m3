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

package ingest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLatencyBuckets(t *testing.T) {
	buckets, err := NewLatencyBuckets()
	require.NoError(t, err)

	// NB(r): Bucket length is tested just to sanity check how many buckets we are creating
	require.Equal(t, 50, len(buckets.WriteLatencyBuckets.AsDurations()))

	// NB(r): Bucket values are tested to sanity check they look right
	// nolint: lll
	expected := "[0s 100ms 200ms 300ms 400ms 500ms 600ms 700ms 800ms 900ms 1s 1.5s 2s 2.5s 3s 3.5s 4s 4.5s 5s 5.5s 6s 6.5s 7s 7.5s 8s 8.5s 9s 9.5s 10s 15s 20s 25s 30s 35s 40s 45s 50s 55s 1m0s 5m0s 10m0s 15m0s 20m0s 25m0s 30m0s 35m0s 40m0s 45m0s 50m0s 55m0s]"
	actual := fmt.Sprintf("%v", buckets.WriteLatencyBuckets.AsDurations())
	require.Equal(t, expected, actual)

	// NB(r): Bucket length is tested just to sanity check how many buckets we are creating
	require.Equal(t, 80, len(buckets.IngestLatencyBuckets.AsDurations()))

	// NB(r): Bucket values are tested to sanity check they look right
	// nolint: lll
	expected = "[0s 100ms 200ms 300ms 400ms 500ms 600ms 700ms 800ms 900ms 1s 1.5s 2s 2.5s 3s 3.5s 4s 4.5s 5s 5.5s 6s 6.5s 7s 7.5s 8s 8.5s 9s 9.5s 10s 15s 20s 25s 30s 35s 40s 45s 50s 55s 1m0s 5m0s 10m0s 15m0s 20m0s 25m0s 30m0s 35m0s 40m0s 45m0s 50m0s 55m0s 1h0m0s 1h30m0s 2h0m0s 2h30m0s 3h0m0s 3h30m0s 4h0m0s 4h30m0s 5h0m0s 5h30m0s 6h0m0s 6h30m0s 7h0m0s 8h0m0s 9h0m0s 10h0m0s 11h0m0s 12h0m0s 13h0m0s 14h0m0s 15h0m0s 16h0m0s 17h0m0s 18h0m0s 19h0m0s 20h0m0s 21h0m0s 22h0m0s 23h0m0s 24h0m0s]"
	actual = fmt.Sprintf("%v", buckets.IngestLatencyBuckets.AsDurations())
	require.Equal(t, expected, actual)
}
