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

	buckets := []pool.Bucket{
		{Capacity: 2, Count: pool.Size(-1)},
		{Capacity: 4, Count: pool.Size(2)},
		{Capacity: 8, Count: pool.Size(3)},
	}

	pool := NewMultiReaderIteratorArrayPool(buckets)
	pool.Init()

	arr := pool.Get(4)
	assert.Equal(t, 0, len(arr))
	assert.Equal(t, 4, cap(arr))

	arr = append(arr, nil)
	pool.Put(arr)

	reusedArr := pool.Get(4)
	assert.Equal(t, 0, len(reusedArr))
	assert.Equal(t, 4, cap(reusedArr))

	largeArr := pool.Get(16)
	assert.Equal(t, 0, len(largeArr))
	assert.Equal(t, 16, cap(largeArr))

	pool.Put(largeArr)
}
