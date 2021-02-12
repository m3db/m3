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
	"runtime"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

const (
	_flushEvery             = 10000
	_insertAndCompressEvery = 1000
	_sampleBatches          = 100
	_eps                    = 0.001
	_heapCapacity           = 32
)

var (
	_floatsPool = pool.NewFloatsPool([]pool.Bucket{
		{Capacity: 512, Count: 256},
		{Capacity: 1024, Count: 256},
		{Capacity: 4096, Count: 256},
		{Capacity: 8192, Count: 256},
		{Capacity: 16384, Count: 256},
	}, pool.NewObjectPoolOptions())
	_cmOptions = cm.NewOptions().
			SetFlushEvery(_flushEvery).
			SetInsertAndCompressEvery(_insertAndCompressEvery).
			SetEps(_eps).
			SetCapacity(_heapCapacity).
			SetFloatsPool(_floatsPool)
	_aggregationOptions = NewOptions(instrument.NewOptions())
	_now                = time.Now()
)

func getTimer() Timer {
	return NewTimer(testQuantiles, _cmOptions, _aggregationOptions)
}

func timerSamples() [][]float64 {
	rnd := rand.New(rand.NewSource(0)) //nolint:gosec
	numBatches := _flushEvery / _insertAndCompressEvery
	samples := make([][]float64, numBatches)
	for i := 0; i < len(samples); i++ {
		samples[i] = make([]float64, _insertAndCompressEvery)
		for j := 0; j < _insertAndCompressEvery; j++ {
			samples[i][j] = rnd.ExpFloat64()
		}
	}
	return samples
}

func BenchmarkTimerValues(b *testing.B) {
	timer := getTimer()
	for i := 1; i <= 2500; i++ {
		timer.Add(_now, float64(i))
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
	var samples = timerSamples()

	b.Run("10k samples in 10 batches", func(b *testing.B) {
		benchAddBatch(b, samples)
	})

	b.Run("5k samples in 5 batches", func(b *testing.B) {
		benchAddBatch(b, samples[:5])
	})

	b.Run("1k samples in 1 batch", func(b *testing.B) {
		benchAddBatch(b, samples[:1])
	})
}

func benchAddBatch(b *testing.B, samples [][]float64) {
	var q float64

	b.SetBytes(int64(8 * len(samples) * len(samples[0])))
	for n := 0; n < b.N; n++ {
		timer := getTimer()

		for i := 0; i < len(samples); i++ {
			timer.AddBatch(_now, samples[i])
		}

		for i := range testQuantiles {
			q = timer.Quantile(testQuantiles[i])
		}

		if math.IsNaN(q) {
			b.FailNow()
		}
	}

	runtime.KeepAlive(q)
}

func init() {
	_aggregationOptions.ResetSetData(testAggTypes)
	_floatsPool.Init()
}
