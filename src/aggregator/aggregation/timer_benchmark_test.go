// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregation

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	_insertAndCompressEvery = 1024
	_eps                    = 0.001
	_capacity               = 32
)

var (
	_aggregationOptions = NewOptions(instrument.NewOptions())
	_now                = time.Now()
	_cmOptions          cm.Options
)

func getTimer() Timer {
	return NewTimer(testQuantiles, _cmOptions, _aggregationOptions)
}

func BenchmarkTimerValues(b *testing.B) {
	timer := getTimer()
	for i := 1; i <= 2500; i++ {
		timer.Add(_now, float64(i), nil)
	}
	for n := 0; n < b.N; n++ {
		timer.Sum()
		timer.SumSq()
		timer.Mean()
		timer.Min()
		timer.Max()
		timer.Count()
		timer.Stdev()
		timer.Quantile(0.5)
		timer.Quantile(0.5)
		timer.Quantile(0.95)
		timer.Quantile(0.99)
	}
}

func BenchmarkTimerValueOf(b *testing.B) {
	timer := getTimer()
	for n := 0; n < b.N; n++ {
		for _, aggType := range testAggTypes {
			timer.ValueOf(aggType)
		}
	}
}

func BenchmarkTimerAddBatch(b *testing.B) {
	for _, bench := range []struct {
		name      string
		num       int
		batchSize int
		sampleFn  func(*rand.Rand) float64
	}{
		{
			name:      "100k samples 1000 batch size",
			num:       100_000,
			batchSize: 1000,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64()
			},
		},
		{
			name:      "100k samples 1000 batch size 0..1,000,000 range",
			num:       100_000,
			batchSize: 1000,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64() * 1_000_000
			},
		},
		{
			name:      "100k samples 1000 batch size 0..100,000 range",
			num:       100_000,
			batchSize: 1000,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64() * 100_000
			},
		},
		{
			name:      "10k samples 1000 batch size",
			num:       10_000,
			batchSize: 1000,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64()
			},
		},
		{
			name:      "10k samples 100 batch size",
			num:       10_000,
			batchSize: 100,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64()
			},
		},
		{
			name:      "10k samples 100 batch size with negative values",
			num:       10_000,
			batchSize: 100,
			sampleFn: func(r *rand.Rand) float64 {
				return -1.0*r.Float64() + 10.0
			},
		},
		{
			name:      "1k samples 100 batch size",
			num:       1_000,
			batchSize: 100,
			sampleFn: func(r *rand.Rand) float64 {
				return r.Float64()
			},
		},
	} {
		bench := bench
		samples, q := getTimerSamples(bench.num, bench.sampleFn, testQuantiles)
		b.Run(bench.name, func(b *testing.B) {
			benchAddBatch(b, samples, bench.batchSize, testQuantiles, q)
		})
	}
}

func benchAddBatch(
	b *testing.B,
	samples []float64,
	batchSize int,
	quantiles []float64,
	quantileValues []float64,
) {
	b.Helper()
	b.SetBytes(int64(8 * len(samples)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			timer := getTimer()

			for i := 0; i < len(samples); i += batchSize {
				end := i + batchSize
				if end >= len(samples) {
					end = len(samples) - 1
				}
				timer.AddBatch(_now, samples[i:end], nil)
			}

			for i := range quantiles {
				q := timer.Quantile(quantiles[i])
				// error bound is quantile * epsilon
				expected := (_eps / quantiles[i]) * quantileValues[i]
				delta := math.Abs(q - quantileValues[i])
				if delta > expected {
					b.Logf("unexpected delta: value %f, expected delta %v, delta %f, quantile %f",
						q, expected, delta, quantiles[i])
					b.FailNow()
				}
			}
			timer.Close()
		}
	})
}

func init() {
	_aggregationOptions.ResetSetData(testAggTypes)
	_cmOptions = cm.NewOptions().
		SetInsertAndCompressEvery(_insertAndCompressEvery).
		SetEps(_eps).
		SetCapacity(_capacity)
	_aggregationOptions.ResetSetData(testAggTypes)
}
