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

package writer

import (
	"testing"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewShardedWriter(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	w1, w2 := NewMockWriter(ctrl), NewMockWriter(ctrl)
	writers := []Writer{w1, w2}

	shardFn := func(_ id.ChunkedID, _ int) uint32 {
		return 0
	}

	w, err := NewShardedWriter(writers, shardFn, instrument.NewOptions())
	require.NoError(t, err)

	metric := aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: id.ChunkedID{
				Data: []byte("some-random-id"),
			},
		},
	}

	w1.EXPECT().Write(metric).Return(nil)
	require.NoError(t, w.Write(metric))

	w1.EXPECT().Flush().Return(nil)
	w2.EXPECT().Flush().Return(nil)
	require.NoError(t, w.Flush())

	w1.EXPECT().Close().Return(nil)
	w2.EXPECT().Close().Return(nil)
	require.NoError(t, w.Close())
}
