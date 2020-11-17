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

package buffer

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestManager(
	bufferCapacity int,
	bufferCount int,
	timeout time.Duration,
) FixedBufferManager {
	opts := NewOptions().
		SetFixedBufferCapacity(bufferCapacity).
		SetFixedBufferCount(bufferCount)

	if timeout > 0 {
		opts = opts.SetFixedBufferTimeout(timeout)
	}

	return newFixedBufferManager(opts)
}

func TestBufferManager(t *testing.T) {
	var (
		bufferCount    = 2
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, 0)

		dataOne   = []byte{1, 2, 3}
		dataTwo   = []byte{4, 5}
		dataThree = []byte{6, 7, 8, 9, 10}
	)

	bOne, leaseOne, err := manager.Copy(dataOne)
	require.NoError(t, err)
	assert.False(t, xtest.ByteSlicesBackedBySameData(dataOne, bOne))

	bTwo, leaseTwo, err := manager.Copy(dataTwo)
	require.NoError(t, err)
	assert.False(t, xtest.ByteSlicesBackedBySameData(dataTwo, bTwo))

	bThree, leaseThree, err := manager.Copy(dataThree)
	require.NoError(t, err)
	assert.False(t, xtest.ByteSlicesBackedBySameData(dataThree, bThree))

	assert.Equal(t, dataOne, bOne)
	assert.Equal(t, dataTwo, bTwo)
	assert.Equal(t, []byte{6, 7, 8, 9, 10}, bThree)

	leaseThree.Finalize()
	// free third lease and copy in new data; existing buffers should change.
	_, leaseRewrite, err := manager.Copy([]byte{60, 70, 80, 90, 100})
	require.NoError(t, err)
	assert.Equal(t, []byte{60, 70, 80, 90, 100}, bThree)

	// free first and second lease and copy in new data; existing buffers should change.
	leaseOne.Finalize()
	leaseTwo.Finalize()

	leaseRewrite.Finalize()
	_, leaseRewrite, err = manager.Copy([]byte{10, 20, 30, 40, 50})
	require.NoError(t, err)
	assert.Equal(t, []byte{10, 20, 30}, bOne)
	assert.Equal(t, []byte{40, 50}, bTwo)
	assert.Equal(t, []byte{60, 70, 80, 90, 100}, bThree)
	leaseRewrite.Finalize()
}

func TestBufferManagerUnbufferedRequests(t *testing.T) {
	var (
		bufferCount    = 2
		bufferCapacity = 3
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)

		data     = []byte{1, 2, 3}
		longData = []byte{1, 2, 3, 4, 5, 6}
	)

	longBuffered, longLease, err := manager.Copy(longData)
	require.NoError(t, err)
	assert.False(t, xtest.ByteSlicesBackedBySameData(longData, longBuffered))
	assert.Equal(t, longData, longBuffered)

	buffered, lease, err := manager.Copy(data)
	require.NoError(t, err)
	assert.False(t, xtest.ByteSlicesBackedBySameData(data, buffered))
	assert.Equal(t, data, buffered)

	_, _, err = manager.Copy([]byte{4, 5, 6})
	require.Error(t, err)

	longLease.Finalize()
	lease.Finalize()
}

func TestFixedBufferGetTimeout(t *testing.T) {
	var (
		bufferCount    = 3
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)
	)

	addTwoSmallOneLarge := func() []Lease {
		bufferLease := []Lease{}
		_, lease, err := manager.Copy(make([]byte, 1))
		require.NoError(t, err)
		bufferLease = append(bufferLease, lease)

		_, lease, err = manager.Copy(make([]byte, 1))
		require.NoError(t, err)
		bufferLease = append(bufferLease, lease)

		_, lease, err = manager.Copy(make([]byte, bufferCapacity*2))
		require.NoError(t, err)
		bufferLease = append(bufferLease, lease)
		return bufferLease
	}

	addLarge := func() Lease {
		_, lease, err := manager.Copy(make([]byte, bufferCapacity*2))
		require.NoError(t, err)
		return lease
	}

	ensureCannotAllocate := func() {
		// NB: ensure that cannot allocate small or large lease.
		_, _, err := manager.Copy(make([]byte, 1))
		require.Error(t, err)

		_, _, err = manager.Copy(make([]byte, bufferCapacity*2))
		require.Error(t, err)
	}

	firstBufferLeases := addTwoSmallOneLarge()
	secondBufferLeases := addTwoSmallOneLarge()
	thirdBufferLease := addLarge()

	ensureCannotAllocate()

	// NB: clear the small leases from the first lease and
	// the large lease from the second. Should still not be able to allocate.
	firstBufferLeases[0].Finalize()
	firstBufferLeases[1].Finalize()
	secondBufferLeases[2].Finalize()
	ensureCannotAllocate()

	// Ensure can release large lease and take another lease.
	thirdBufferLease.Finalize()
	thirdBufferLease = addLarge()
	ensureCannotAllocate()

	// Ensure can release large lease from first lease to receive another lease.
	firstBufferLeases[2].Finalize()
	fourthBufferLease := addLarge()
	ensureCannotAllocate()

	// Ensure can release small leases from second leases to receive another lease.
	secondBufferLeases[0].Finalize()
	secondBufferLeases[1].Finalize()
	fifthBufferLease := addLarge()
	ensureCannotAllocate()

	// Cleanup.
	thirdBufferLease.Finalize()
	fourthBufferLease.Finalize()
	fifthBufferLease.Finalize()
}

func TestTimeoutBuffersAvailableWhenCompletingWithinTimeout(t *testing.T) {
	testTimeoutBuffersAvailableWhenCompleted(t, time.Second)
}

func TestTimeoutBuffersAvailableWhenCompletingWithoutTimeout(t *testing.T) {
	testTimeoutBuffersAvailableWhenCompleted(t, 0)
}

func testTimeoutBuffersAvailableWhenCompleted(t *testing.T, timeout time.Duration) {
	var (
		bufferCount    = 3
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, timeout)

		wg sync.WaitGroup
	)

	for i := 0; i < bufferCount*5; i++ {
		wg.Add(1)
		_, lease, err := manager.Copy(make([]byte, bufferCapacity))
		require.NoError(t, err)
		go func() {
			// NB: simulate some work that is completed before timeout completes.
			time.Sleep(time.Millisecond * 20)
			lease.Finalize()
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestAllocPartialBuffer(t *testing.T) {
	var (
		bufferCount    = 1
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)
	)

	_, firstLease, err := manager.Copy(make([]byte, 4))
	require.NoError(t, err)
	_, secondLease, err := manager.Copy(make([]byte, 1))
	require.NoError(t, err)

	_, _, err = manager.Copy(make([]byte, 1))
	require.Error(t, err)

	firstLease.Finalize()
	_, firstLease, err = manager.Copy(make([]byte, 2))
	_, thirdLease, err := manager.Copy(make([]byte, 2))

	_, _, err = manager.Copy(make([]byte, 1))
	require.Error(t, err)

	firstLease.Finalize()
	secondLease.Finalize()
	thirdLease.Finalize()
}

func TestAllocPartialBufferAfterAnotherStarted(t *testing.T) {
	var (
		bufferCount    = 2
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)
	)

	_, firstLease, err := manager.Copy(make([]byte, 3))
	require.NoError(t, err)
	_, secondLease, err := manager.Copy(make([]byte, 1))
	require.NoError(t, err)
	_, thirdLease, err := manager.Copy(make([]byte, 3))
	require.NoError(t, err)
	_, fourthLease, err := manager.Copy(make([]byte, 2))
	require.NoError(t, err)

	_, _, err = manager.Copy(make([]byte, 1))
	require.Error(t, err)

	firstLease.Finalize()
	_, _, err = manager.Copy(make([]byte, 1))
	require.Error(t, err)

	secondLease.Finalize()
	_, firstLease, err = manager.Copy(make([]byte, 1))
	_, secondLease, err = manager.Copy(make([]byte, 4))

	firstLease.Finalize()
	secondLease.Finalize()
	thirdLease.Finalize()
	fourthLease.Finalize()
}

func anyMultiBytesBackedBySameData(t *testing.T, expected, actual [][]byte) bool {
	expectedHeader := (*reflect.SliceHeader)(unsafe.Pointer(&expected))
	actualHeader := (*reflect.SliceHeader)(unsafe.Pointer(&actual))
	if expectedHeader == actualHeader {
		fmt.Println("top level same.")
		return true
	}

	require.Equal(t, len(expected), len(actual), "backed bytes length mismatch")
	for i, ex := range expected {
		if xtest.ByteSlicesBackedBySameData(ex, actual[i]) {
			return true
		}
	}

	return false
}

func TestAllocMultiBuffer(t *testing.T) {
	var (
		bufferCount    = 1
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)

		buffer      [][]byte
		firstMulti  = [][]byte{{1}, {2}, {3}}
		secondMulti = [][]byte{{4}, {5}}
	)

	firstCopied, firstLease, err := manager.MultiCopy(buffer, firstMulti...)
	require.NoError(t, err)
	assert.Equal(t, firstMulti, firstCopied)
	assert.False(t, anyMultiBytesBackedBySameData(t, firstMulti, firstCopied))

	secondCopied, secondLease, err := manager.MultiCopy(buffer, secondMulti...)
	require.NoError(t, err)
	assert.Equal(t, secondMulti, secondCopied)
	assert.False(t, anyMultiBytesBackedBySameData(t, secondMulti, secondCopied))

	_, _, err = manager.Copy([]byte{1})
	require.Error(t, err)

	firstLease.Finalize()
	secondLease.Finalize()

	thirdCopied, thirdLease, err := manager.MultiCopy(buffer, []byte{9, 8, 7}, []byte{6, 5})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{{9, 8, 7}, {6, 5}}, thirdCopied)
	thirdLease.Finalize()
}

func TestAllocMixedBuffer(t *testing.T) {
	var (
		bufferCount    = 1
		bufferCapacity = 5
		manager        = buildTestManager(bufferCapacity, bufferCount, time.Millisecond*20)

		buffer     [][]byte
		firstMulti = [][]byte{{1}, {2}, {3}}
		second     = []byte{4, 5}
	)

	firstCopied, firstLease, err := manager.MultiCopy(buffer, firstMulti...)
	require.NoError(t, err)
	assert.Equal(t, firstMulti, firstCopied)
	assert.False(t, anyMultiBytesBackedBySameData(t, firstMulti, firstCopied))

	secondCopied, secondLease, err := manager.Copy(second)
	require.NoError(t, err)
	assert.Equal(t, second, secondCopied)

	_, _, err = manager.Copy([]byte{1})
	require.Error(t, err)

	firstLease.Finalize()
	secondLease.Finalize()

	thirdCopied, thirdLease, err := manager.MultiCopy(buffer, []byte{9, 8, 7}, []byte{6, 5})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{{9, 8, 7}, {6, 5}}, thirdCopied)
	thirdLease.Finalize()
}
