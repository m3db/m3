package encoding

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/x/pool"
)

func TestMultiReaderIteratorArrayPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Define pool bucket sizes
	buckets := []pool.Bucket{
		{Capacity: 2, Count: pool.Size(-1)},
		{Capacity: 4, Count: pool.Size(2)},
		{Capacity: 8, Count: pool.Size(3)},
	}

	// Create the pool
	pool := NewMultiReaderIteratorArrayPool(buckets)
	pool.Init()

	// Test Get() - requesting a capacity within bucket limits
	arr := pool.Get(4)
	assert.Equal(t, 0, len(arr))
	assert.Equal(t, 4, cap(arr)) // Should match bucket capacity

	// Test Put() - returning an array to the pool
	arr = append(arr, nil) // Simulate use
	pool.Put(arr)

	// Test Get() - retrieving the same array
	reusedArr := pool.Get(4)
	assert.Equal(t, 0, len(reusedArr))
	assert.Equal(t, 4, cap(reusedArr)) // Should be the same bucket

	// Test Get() with an oversized request
	largeArr := pool.Get(16)
	assert.Equal(t, 0, len(largeArr))
	assert.Equal(t, 16, cap(largeArr)) // Should allocate new since it's out of range

	// Test Put() with an oversized array (should not be stored)
	pool.Put(largeArr)
}
