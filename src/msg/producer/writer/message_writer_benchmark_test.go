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

package writer

import (
	"testing"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/instrument"
)

func BenchmarkScanMessageQueue(b *testing.B) {
	opts := testOptions().
		SetMessageQueueScanBatchSize(128).
		SetInstrumentOptions(
			instrument.NewOptions().SetTimerOptions(
				instrument.TimerOptions{
					Type:               instrument.StandardTimerType,
					StandardSampleRate: 0.05,
				},
			),
		)

	b.RunParallel(func(pb *testing.PB) {
		w := newMessageWriter(
			200,
			newMessagePool(),
			opts,
			testMessageWriterMetrics(),
		)

		w.consumerWriters = append(w.consumerWriters, noopWriter{})

		for i := 0; i < 1024; i++ {
			w.Write(producer.NewRefCountedMessage(emptyMessage{}, nil))
		}
		b.ResetTimer()

		for pb.Next() {
			w.scanMessageQueue()
			if w.QueueSize() != 1024 {
				b.Fatalf("expected queue len to be 1024, got %v", w.QueueSize())
			}
		}
	})
}

type noopWriter struct{}

func (noopWriter) Address() string         { return "" }
func (noopWriter) Write(int, []byte) error { return nil }
func (noopWriter) Init()                   {}
func (noopWriter) Close()                  {}
