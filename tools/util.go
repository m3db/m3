package tools

import (
	"github.com/m3db/m3x/pool"
)

// NewCheckedBytesPool returns a configured (and initialized)
// CheckedBytesPool with default pool values
func NewCheckedBytesPool() pool.CheckedBytesPool {
	bytesPoolOpts := pool.NewObjectPoolOptions().
		SetRefillLowWatermark(0.05).
		SetRefillHighWatermark(0.07)

	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		pool.Bucket{
			Capacity: 16,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 32,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 64,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 128,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 256,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 1440,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 4096,
			Count:    262144,
		},
		pool.Bucket{
			Capacity: 8192,
			Count:    65536,
		},
	}, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, bytesPoolOpts)
	})
	bytesPool.Init()
	return bytesPool
}
