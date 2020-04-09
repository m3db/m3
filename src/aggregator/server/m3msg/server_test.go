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

package m3msg

import (
	"context"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	xm3msg "github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
)

var (
	defaultStoragePolicy = policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour)

	expectedMetric = aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        []byte("FakeMetricID"),
		TimeNanos: 0,
		Value:     2.333,
	}

	expectedMetadata = metadata.TimedMetadata{
		StoragePolicy: defaultStoragePolicy,
	}
)

func TestNewPassThroughWriteFn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	aggregator := aggregator.NewMockAggregator(ctrl)
	writeFn := newPassThroughWriteFn(aggregator)
	callback := xm3msg.NewMockCallbackable(ctrl)
	callback.EXPECT().Callback(xm3msg.OnSuccess).Times(1)
	aggregator.EXPECT().AddPassThrough(expectedMetric, expectedMetadata).Return(nil).Times(1)

	writeFn(
		context.TODO(),
		expectedMetric.ID,
		expectedMetric.TimeNanos,
		expectedMetric.TimeNanos,
		expectedMetric.Value,
		defaultStoragePolicy,
		callback,
	)
}
