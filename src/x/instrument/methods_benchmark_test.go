// Copyright (c) 2021 Uber Technologies, Inc.
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

package instrument

import (
	"runtime"
	"testing"
	"time"

	"github.com/uber-go/tally"
)

func BenchmarkSampledTimer(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchRecord(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerLowRate(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchRecord(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerStopwatch(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.5)
	benchStopwatch(b, tm)
	runtime.KeepAlive(tm)
}

func BenchmarkSampledTimerStopwatchLowRate(b *testing.B) {
	tm := MustCreateSampledTimer(tally.NoopScope.Timer("test"), 0.01)
	benchStopwatch(b, tm)
	runtime.KeepAlive(tm)
}

func benchStopwatch(b *testing.B, tm tally.Timer) {
	b.RunParallel(func(pb *testing.PB) {
		var sw tally.Stopwatch
		for pb.Next() {
			sw = tm.Start()
			sw.Stop()
		}
		runtime.KeepAlive(sw)
	})
}

func benchRecord(b *testing.B, tm tally.Timer) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tm.Record(1 * time.Second)
		}
	})
}
