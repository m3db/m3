// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"runtime"
	"sync"
	"testing"
)

const baseline = 1024

func BenchmarkEnqueueChannel(b *testing.B) {
	work := baseline * b.N
	workerTarget := work / runtime.GOMAXPROCS(0)
	actual := 0
	// expected = (((n^2)+n)/2) * workers
	expected := (((workerTarget * workerTarget) + workerTarget) / 2) * runtime.GOMAXPROCS(0)

	q := make(chan int, work/runtime.GOMAXPROCS(0))

	flushingL := sync.Mutex{}

	flushQ := func() {
		flushingL.Lock()
		for {
			select {
			case v := <-q:
				actual += v
			default:
				flushingL.Unlock()
				return
			}
		}
	}

	var wg sync.WaitGroup

	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			for i := 0; i < workerTarget; i++ {
				select {
				case q <- i + 1:
					continue
				default:
					go flushQ()
					q <- i + 1
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	flushQ()

	if actual != expected {
		b.Fatalf("failed to compute, actual %d, expected %d", actual, expected)
	}
}

func BenchmarkEnqueueMutex(b *testing.B) {
	work := baseline * b.N
	workerTarget := work / runtime.GOMAXPROCS(0)
	actual := 0
	// expected = (((n^2)+n)/2) * workers
	expected := (((workerTarget * workerTarget) + workerTarget) / 2) * runtime.GOMAXPROCS(0)

	q := make([]int, 0, work/runtime.GOMAXPROCS(0))
	qCopy := make([]int, 0, work/runtime.GOMAXPROCS(0))
	l := sync.RWMutex{}
	flushingL := sync.Mutex{}

	flushQ := func() {
		qCopyLen := len(qCopy)
		for i := 0; i < qCopyLen; i++ {
			actual += qCopy[i]
		}
		qCopy = qCopy[:0]
	}

	var wg sync.WaitGroup

	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			for i := 0; i < workerTarget; i++ {
				l.Lock()
				q = append(q, i+1)
				if len(q) == workerTarget {
					flushingL.Lock()
					// Swap the copy and start flushing
					q, qCopy = qCopy, q
					go func() {
						flushQ()
						flushingL.Unlock()
					}()
				}
				l.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	flushingL.Lock()
	flushQ()
	flushingL.Unlock()

	if actual != expected {
		b.Fatalf("failed to compute, actual %d, expected %d", actual, expected)
	}
}
