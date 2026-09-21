package writes

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/pool"
)

func TestWriteBatchPool_GetAndPut(t *testing.T) {
	opts := pool.NewObjectPoolOptions()
	initialBatchSize := 10
	maxBatchSizeOverride := 100
	ctrl := gomock.NewController(t)

	opts = opts.SetSize(2)
	writeBatchPool := NewWriteBatchPool(opts, initialBatchSize, &maxBatchSizeOverride)
	mockBatch := NewMockWriteBatch(ctrl)
	mockBatch.EXPECT().cap().Return(initialBatchSize).Times(2)

	// Intercept Init with a controlled mock
	writeBatchPool.pool = pool.NewObjectPool(opts)
	writeBatchPool.pool.Init(func() interface{} {
		return mockBatch
	})

	// Get returns a batch
	wb := writeBatchPool.Get()
	require.Equal(t, initialBatchSize, wb.cap())

	// Put a batch within allowed size
	writeBatchPool.Put(mockBatch) // should be added back to pool

	// Put a batch over max size
	mockBatch1 := NewMockWriteBatch(ctrl)
	mockBatch1.EXPECT().cap().Return(200)
	writeBatchPool.Put(mockBatch1) // should NOT be added back
}

func TestWriteBatchPool_OverrideMaxBatchSize(t *testing.T) {
	override := 2048
	pool := NewWriteBatchPool(pool.NewObjectPoolOptions(), 10, &override)
	require.Equal(t, override, pool.maxBatchSize)
}

func TestWriteBatchPool_DefaultMaxBatchSize(t *testing.T) {
	pool := NewWriteBatchPool(pool.NewObjectPoolOptions(), 10, nil)
	require.Equal(t, defaultMaxBatchSize, pool.maxBatchSize)
}
