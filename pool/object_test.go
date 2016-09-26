// Copyright (c) 2016 Uber Technologies, Inc.
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

package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestObjectPoolRefillOnLowWaterMark(t *testing.T) {
	opts := NewObjectPoolOptions().
		SetSize(100).
		SetRefillLowWatermark(0.25).
		SetRefillHighWatermark(0.75)

	pool := NewObjectPool(opts).(*objectPool)
	pool.Init(func() interface{} {
		return 1
	})

	assert.Equal(t, 100, len(pool.values))

	for i := 0; i < 74; i++ {
		pool.Get()
	}

	assert.Equal(t, 26, len(pool.values))

	// This should trigger a refill
	pool.Get()

	start := time.Now()
	for time.Now().Sub(start) < 10*time.Second {
		if len(pool.values) == 75 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Assert refilled
	assert.Equal(t, 75, len(pool.values))
}
