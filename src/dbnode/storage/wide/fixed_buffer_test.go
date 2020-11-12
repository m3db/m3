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

package wide

import (
	"testing"
	"time"

	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferManager(t *testing.T) {
	var (
		bufferCount    = 2
		bufferCapacity = 5
		opts           = NewOptions().
				SetFixedBufferCapacity(bufferCapacity).
				SetFixedBufferCount(bufferCount)
		manager = newFixedBufferManager(opts)

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
		bufferCount    = 1
		bufferCapacity = 3
		opts           = NewOptions().
				SetFixedBufferCapacity(bufferCapacity).
				SetFixedBufferCount(bufferCount).
				SetFixedBufferTimeout(time.Millisecond * 20)

		manager = newFixedBufferManager(opts)

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
