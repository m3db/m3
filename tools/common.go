package tools

import (
	"github.com/m3db/m3x/pool"
)

// NewCheckedBytesPool returns a configured (and initialized)
// CheckedBytesPool with default pool values
func NewCheckedBytesPool() pool.CheckedBytesPool {
	bytesPoolOpts := pool.NewObjectPoolOptions().
		SetRefillLowWatermark(0.001).
		SetRefillHighWatermark(0.002)

	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		pool.Bucket{
			Capacity: 16,
			Count:    6291456,
		},
		pool.Bucket{
			Capacity: 32,
			Count:    3145728,
		},
		pool.Bucket{
			Capacity: 64,
			Count:    3145728,
		},
		pool.Bucket{
			Capacity: 128,
			Count:    3145728,
		},
		pool.Bucket{
			Capacity: 256,
			Count:    3145728,
		},
		pool.Bucket{
			Capacity: 1440,
			Count:    524288,
		},
		pool.Bucket{
			Capacity: 4096,
			Count:    524288,
		},
		pool.Bucket{
			Capacity: 8192,
			Count:    524288,
		},
	}, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, bytesPoolOpts)
	})
	bytesPool.Init()
	return bytesPool
}
