// Copyright (c) 2024 Uber Technologies, Inc.
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

package block

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/pool"
)

func TestDatabaseBlockPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("NewDatabaseBlockPool", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseBlockPool(opts)
		require.NotNil(t, p)
	})

	t.Run("Init", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseBlockPool(opts)

		// Create a mock block for allocation
		mockBlock := NewMockDatabaseBlock(ctrl)
		allocCount := 0

		// Initialize the pool with an allocator that counts allocations
		p.Init(func() DatabaseBlock {
			allocCount++
			return mockBlock
		})

		// Get a block from the pool, should trigger allocation
		block := p.Get()
		require.Equal(t, mockBlock, block)
		require.Equal(t, 1, allocCount)

		// Put the block back
		p.Put(block)

		// Get the block again, should not trigger allocation
		block = p.Get()
		require.Equal(t, mockBlock, block)
		require.Equal(t, 1, allocCount)
	})

	t.Run("GetPut", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(2)
		p := NewDatabaseBlockPool(opts)

		// Create mock blocks
		mockBlock1 := NewMockDatabaseBlock(ctrl)
		mockBlock2 := NewMockDatabaseBlock(ctrl)
		nextBlock := mockBlock1

		// Initialize the pool
		p.Init(func() DatabaseBlock {
			if nextBlock == mockBlock1 {
				nextBlock = mockBlock2
				return mockBlock1
			}
			return mockBlock2
		})

		// Get first block
		block1 := p.Get()
		require.Equal(t, mockBlock1, block1)

		// Get second block
		block2 := p.Get()
		require.Equal(t, mockBlock2, block2)

		// Put blocks back
		p.Put(block1)
		p.Put(block2)

		// Get blocks again, should get the same blocks
		block1Again := p.Get()
		require.Equal(t, mockBlock2, block1Again)
		block2Again := p.Get()
		require.Equal(t, mockBlock1, block2Again)
	})

	t.Run("Reuse", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseBlockPool(opts)

		// Create a mock block
		mockBlock := NewMockDatabaseBlock(ctrl)
		allocCount := 0

		// Initialize the pool
		p.Init(func() DatabaseBlock {
			allocCount++
			return mockBlock
		})

		// Get and put multiple times
		for i := 0; i < 10; i++ {
			block := p.Get()
			require.Equal(t, mockBlock, block)
			p.Put(block)
		}

		// Should only have allocated once
		require.Equal(t, 1, allocCount)
	})

	t.Run("Capacity", func(t *testing.T) {
		size := 2
		opts := pool.NewObjectPoolOptions().SetSize(size)
		p := NewDatabaseBlockPool(opts)

		// Create mock blocks
		mockBlock1 := NewMockDatabaseBlock(ctrl)
		mockBlock2 := NewMockDatabaseBlock(ctrl)
		nextBlock := mockBlock1

		// Initialize the pool
		p.Init(func() DatabaseBlock {
			if nextBlock == mockBlock1 {
				nextBlock = mockBlock2
				return mockBlock1
			}
			return mockBlock2
		})

		// Get blocks up to capacity
		blocks := make([]DatabaseBlock, size)
		for i := 0; i < size; i++ {
			blocks[i] = p.Get()
		}

		// Put all blocks back
		for i := 0; i < size; i++ {
			p.Put(blocks[i])
		}

		// Get blocks again
		for i := 0; i < size; i++ {
			block := p.Get()
			require.NotNil(t, block)
		}
	})
}
