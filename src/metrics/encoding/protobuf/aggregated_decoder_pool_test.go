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

package protobuf

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestAggregatedDecoderPool(t *testing.T) {
	opts := pool.NewObjectPoolOptions().SetSize(1)

	pool := NewAggregatedDecoderPool(opts)
	pool.Init()

	// Getting a decoder from pool.
	d := pool.Get().(*aggregatedDecoder)
	require.Empty(t, d.pb.Metric.TimedMetric.Id)
	require.Empty(t, cap(d.pb.Metric.TimedMetric.Id))
	d.pb.Metric.TimedMetric.Id = []byte("foo")

	// Close should reset the internal fields and put it back to pool.
	d.Close()
	require.Empty(t, len(d.pb.Metric.TimedMetric.Id))
	require.NotEmpty(t, cap(d.pb.Metric.TimedMetric.Id))

	// Get will return the previously used decoder.
	newDecoder := pool.Get().(*aggregatedDecoder)
	require.Empty(t, len(newDecoder.pb.Metric.TimedMetric.Id))
	require.NotEmpty(t, cap(newDecoder.pb.Metric.TimedMetric.Id))
}
