// Copyright (c) 2021  Uber Technologies, Inc.
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

package net

import (
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"

	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCoordinatorClient(t *testing.T) {
	logger, err := zap.NewProduction()
	require.NoError(t, err)

	client := resources.NewCoordinatorClient(resources.CoordinatorClientOptions{
		Client:    http.DefaultClient,
		HTTPPort:  7201,
		Logger:    logger,
		RetryFunc: resources.Retry,
	})

	sampleSize := 190
	go func() {
		emitMetric(t, &client, "biz", sampleSize)
	}()

	go func() {
		emitMetric(t, &client, "tech", sampleSize)
	}()

	time.Sleep(1 * time.Hour)
}

func emitMetric(t *testing.T, client *resources.CoordinatorClient, name string, sampleSize int) {
	for i := 0; true; i++ {
		samples := make([]prompb.Sample, 0, sampleSize)
		now := storage.TimeToPromTimestamp(xtime.Now())
		for j := 0; j < sampleSize; j++ {
			samples = append(samples, prompb.Sample{
				Value:     float64(i*sampleSize + j),
				Timestamp: now,
			})
		}
		err := client.WritePromWithRequest(prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  []byte("__g0__"),
							Value: []byte(name),
						},
						{
							Name:  []byte("__g1__"),
							Value: []byte("timers"),
						},
						{
							Name:  []byte("__g2__"),
							Value: []byte("foo"),
						},
					},
					Samples: samples,
					M3Type:  prompb.M3Type_M3_TIMER,
					Source:  prompb.Source_GRAPHITE,
				},
			},
		}, nil)
		require.NoError(t, err)
	}
}
