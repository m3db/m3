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

package tools

import (
	"github.com/m3db/m3/src/x/pool"
)

// NewCheckedBytesPool returns a configured (and initialized)
// CheckedBytesPool with default pool values
func NewCheckedBytesPool() pool.CheckedBytesPool {
	bytesPoolOpts := pool.NewObjectPoolOptions().
		SetRefillLowWatermark(0.05).
		SetRefillHighWatermark(0.07)

	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		{
			Capacity: 16,
			Count:    262144,
		},
		{
			Capacity: 32,
			Count:    262144,
		},
		{
			Capacity: 64,
			Count:    262144,
		},
		{
			Capacity: 128,
			Count:    262144,
		},
		{
			Capacity: 256,
			Count:    262144,
		},
		{
			Capacity: 1440,
			Count:    262144,
		},
		{
			Capacity: 4096,
			Count:    262144,
		},
		{
			Capacity: 8192,
			Count:    65536,
		},
	}, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, bytesPoolOpts)
	})
	bytesPool.Init()
	return bytesPool
}
