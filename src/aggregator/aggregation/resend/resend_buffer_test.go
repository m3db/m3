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

package resend

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxResendBuffer(t *testing.T) {
	var (
		numToAdd   = 10
		bufferSize = 4

		pool = NewBufferPool(BufferPoolOptions{
			PoolSize:          1,
			BufferSize:        bufferSize,
			InstrumentOptions: instrument.NewOptions(),
		})

		maxBuffer = pool.Get()

		inserted        int64
		updated         int64
		updatePersisted float64
	)

	maxBuffer.Reset(true)

	b := maxBuffer.(*buffer)
	assert.True(t, math.IsNaN(maxBuffer.Value()))

	// Insert progressively larger values that should update the max each insert.
	for i := 0; i < numToAdd; i++ {
		inserted++
		floatVal := float64(i)
		maxBuffer.Insert(floatVal)
		require.Equal(t, floatVal, maxBuffer.Value())
	}

	// Insert small values that should not affect the max or change the list.
	currMax := maxBuffer.Value()
	currBuffer := append([]float64{}, b.list...)
	for i := 0; i < numToAdd-bufferSize; i++ {
		inserted++
		floatVal := float64(i)
		maxBuffer.Insert(floatVal)
		require.Equal(t, currMax, maxBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend small values that should not affect the max or change the list.
	for i := 0; i < numToAdd-bufferSize; i++ {
		updated++
		maxBuffer.Update(float64(i), 0)
		require.Equal(t, currMax, maxBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend large values that appear in the list that should update the max.
	for i := numToAdd - bufferSize; i < numToAdd; i++ {
		updated++
		updatePersisted++
		updatedVal := float64(10 + i)
		maxBuffer.Update(float64(i), updatedVal)
		require.Equal(t, updatedVal, maxBuffer.Value())
	}

	// Capture current largest value.
	currMax = maxBuffer.Value()

	// Resend a large value that should not appear in the list.
	smallVal := float64(100)

	updated++
	updatePersisted++
	maxBuffer.Update(0, smallVal)
	require.Equal(t, smallVal, maxBuffer.Value())

	// Update the previously resent large value with a small value to ensure value
	// is returned to max before the large value came in.
	updated++
	updatePersisted++
	maxBuffer.Update(smallVal, 0)
	require.Equal(t, currMax, maxBuffer.Value())

	maxBuffer.Close()
}

func TestMinResendBuffer(t *testing.T) {
	var (
		numToAdd   = 10
		bufferSize = 4

		pool = NewBufferPool(BufferPoolOptions{
			PoolSize:          1,
			BufferSize:        bufferSize,
			InstrumentOptions: instrument.NewOptions(),
		})

		minBuffer = pool.Get()

		inserted        int64
		updated         int64
		updatePersisted float64
	)

	minBuffer.Reset(false)

	b := minBuffer.(*buffer)
	assert.True(t, math.IsNaN(minBuffer.Value()))

	// Insert progressively smaller values that should update the min each insert.
	for i := 0; i < numToAdd; i++ {
		inserted++
		floatVal := float64(numToAdd - i)
		minBuffer.Insert(floatVal)
		require.Equal(t, floatVal, minBuffer.Value())
	}

	// Insert large values that should not affect the min or change the list.
	currMin := minBuffer.Value()
	currBuffer := append([]float64{}, b.list...)
	for i := 0; i < numToAdd-bufferSize; i++ {
		inserted++
		floatVal := float64(numToAdd - i)
		minBuffer.Insert(floatVal)
		require.Equal(t, currMin, minBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend large values that should not affect the min or change the list.
	for i := 0; i < numToAdd-bufferSize; i++ {
		updated++
		minBuffer.Update(float64(numToAdd-i), 100)
		require.Equal(t, currMin, minBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend small values that appear in the list that should update the min.
	for i := numToAdd - bufferSize; i < numToAdd; i++ {
		updated++
		updatePersisted++
		updatedVal := float64(-i)
		minBuffer.Update(float64(i), updatedVal)
		require.Equal(t, updatedVal, minBuffer.Value())
	}

	// Capture current smallest value.
	currMin = minBuffer.Value()

	// Resend a small value that should not appear in the list.
	smallVal := float64(-100)

	updated++
	updatePersisted++
	minBuffer.Update(0, smallVal)
	require.Equal(t, smallVal, minBuffer.Value())

	// Update the previously resent small value with a large value to ensure value
	// is returned to min before the large value came in.
	updated++
	updatePersisted++
	minBuffer.Update(smallVal, 100)
	require.Equal(t, currMin, minBuffer.Value())

	minBuffer.Close()
}
