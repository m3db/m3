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

package sync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testWorkerPoolSize = 5

func TestGo(t *testing.T) {
	var count uint32

	p := NewWorkerPool(testWorkerPoolSize)
	p.Init()

	var wg sync.WaitGroup
	for i := 0; i < testWorkerPoolSize*2; i++ {
		wg.Add(1)
		p.Go(func() {
			atomic.AddUint32(&count, 1)
			wg.Done()
		})
	}
	wg.Wait()

	require.Equal(t, uint32(testWorkerPoolSize*2), count)
}

func TestGoIfAvailable(t *testing.T) {
	var count uint32

	p := NewWorkerPool(testWorkerPoolSize)
	p.Init()

	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < testWorkerPoolSize; i++ {
		wg.Add(1)
		p.Go(func() {
			<-start
			atomic.AddUint32(&count, 1)
			wg.Done()
		})
	}

	// Should fail since no workers are available
	require.False(t, p.GoIfAvailable(func() {}))

	// Release blocked goroutines and wait for completion
	close(start)
	wg.Wait()

	// Spin up another, should execute since there is a worker available
	wg.Add(1)
	p.GoIfAvailable(func() {
		atomic.AddUint32(&count, 1)
		wg.Done()
	})
	wg.Wait()

	require.Equal(t, uint32(testWorkerPoolSize+1), count)
}

func TestGoWithTimeout(t *testing.T) {
	var count uint32

	p := NewWorkerPool(testWorkerPoolSize)
	p.Init()

	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < testWorkerPoolSize; i++ {
		wg.Add(1)
		p.Go(func() {
			<-start
			atomic.AddUint32(&count, 1)
			wg.Done()
		})
	}

	// Should timeout
	require.False(t, p.GoWithTimeout(func() {}, time.Millisecond*500))

	// Release blocked goroutines and wait for completion
	close(start)
	wg.Wait()

	// Spin up another, should execute since there is a worker available
	wg.Add(1)
	p.GoWithTimeout(func() {
		atomic.AddUint32(&count, 1)
		wg.Done()
	}, time.Second*5)
	wg.Wait()

	require.Equal(t, uint32(testWorkerPoolSize+1), count)
}
