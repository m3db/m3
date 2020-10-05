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

package sync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPooledWorkerPoolGo(t *testing.T) {
	var count uint32

	p, err := NewPooledWorkerPool(testWorkerPoolSize, NewPooledWorkerPoolOptions())
	require.NoError(t, err)
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

func TestPooledWorkerPoolGoWithTimeout(t *testing.T) {
	var (
		workers         = 2
		channelCapacity = 1
	)
	// So we can control how empty the worker pool chanel is we
	// set capacity to be same as num workers.
	pooledWorkerPoolGoroutinesCapacity = &channelCapacity
	defer func() {
		pooledWorkerPoolGoroutinesCapacity = nil
	}()

	p, err := NewPooledWorkerPool(workers, NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	p.Init()

	var (
		resultsTrue  = 0
		resultsFalse = 0
		wg           sync.WaitGroup
	)
	wg.Add(1)
	for i := 0; i < workers*2; i++ {
		result := p.GoWithTimeout(func() {
			wg.Wait()
		}, 100*time.Millisecond)
		if result {
			resultsTrue++
		} else {
			resultsFalse++
		}
	}

	wg.Done()

	require.True(t, resultsFalse > 0)
}

func TestPooledWorkerPoolGrowOnDemand(t *testing.T) {
	var count uint32

	p, err := NewPooledWorkerPool(
		1,
		NewPooledWorkerPoolOptions().
			SetGrowOnDemand(true))
	require.NoError(t, err)
	p.Init()

	var (
		wg       sync.WaitGroup
		numIters = testWorkerPoolSize * 2
		doneCh   = make(chan struct{})
	)
	wg.Add(numIters)

	for i := 0; i < numIters; i++ {
		// IfGoOrGrow did not allocate new goroutines then
		// this test would never complete this loop as the
		// anonymous Work function below would not complete
		// and would block further iterations.
		p.Go(func() {
			atomic.AddUint32(&count, 1)
			wg.Done()
			<-doneCh
		})
	}
	close(doneCh)
	wg.Wait()

	require.Equal(t, uint32(numIters), count)
}

func TestPooledWorkerPoolGoOrGrowKillWorker(t *testing.T) {
	var count uint32

	p, err := NewPooledWorkerPool(
		1,
		NewPooledWorkerPoolOptions().
			SetGrowOnDemand(true).
			SetKillWorkerProbability(1.0))
	require.NoError(t, err)
	p.Init()

	var (
		wg       sync.WaitGroup
		numIters = testWorkerPoolSize * 2
		doneCh   = make(chan struct{})
	)
	wg.Add(numIters)

	for i := 0; i < numIters; i++ {
		// IfGoOrGrow did not allocate new goroutines then
		// this test would never complete this loop as the
		// anonymous Work function below would not complete
		// and would block further iterations.
		p.Go(func() {
			atomic.AddUint32(&count, 1)
			wg.Done()
			<-doneCh
		})
	}
	close(doneCh)
	wg.Wait()

	require.Equal(t, uint32(numIters), count)
}

func TestPooledWorkerPoolGoKillWorker(t *testing.T) {
	var count uint32

	p, err := NewPooledWorkerPool(
		testWorkerPoolSize,
		NewPooledWorkerPoolOptions().
			SetKillWorkerProbability(1.0))
	require.NoError(t, err)
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

func TestPooledWorkerPoolSizeTooSmall(t *testing.T) {
	_, err := NewPooledWorkerPool(0, NewPooledWorkerPoolOptions())
	require.Error(t, err)
}
