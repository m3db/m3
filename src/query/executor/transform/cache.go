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

package transform

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/parser"

	"github.com/pkg/errors"
)

// BlockCache is used to cache blocks
type BlockCache struct {
	blocks map[parser.NodeID]block.Block
	mu     sync.Mutex
}

// NewBlockCache creates a new BlockCache
func NewBlockCache() *BlockCache {
	return &BlockCache{
		blocks: make(map[parser.NodeID]block.Block),
	}
}

// Add the block to the cache, errors out if block already exists
func (c *BlockCache) Add(key parser.NodeID, b block.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.blocks[key]
	if ok {
		return errors.New("block already exists")
	}

	c.blocks[key] = b
	return nil
}

// Remove the block from the cache
func (c *BlockCache) Remove(key parser.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocks, key)
}

// Get the block from the cache
// TODO: Evaluate only a single process getting a block at a time
func (c *BlockCache) Get(key parser.NodeID) (block.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blocks[key]
	return b, ok
}

type cacheTime int64

func fromTime(t time.Time) cacheTime {
	return cacheTime(t.UnixNano())
}

func (c cacheTime) toTime() time.Time {
	return time.Unix(0, int64(c))
}

// TimeCache is used to cache blocks over time. It also keeps track of blocks which have been processed
type TimeCache struct {
	blocks          map[cacheTime]block.Block
	processedBlocks map[cacheTime]bool
	mu              sync.Mutex
	initialized     bool
	blockList       []block.Block
}

// NewTimeCache creates a new TimeCache
func NewTimeCache() *TimeCache {
	return &TimeCache{
		blocks:          make(map[cacheTime]block.Block),
		processedBlocks: make(map[cacheTime]bool),
	}
}


// Add the block to the cache, errors out if block already exists
func (c *TimeCache) Add(key time.Time, b block.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.blocks[fromTime(key)]
	if ok {
		return errors.New("block already exists")
	}

	c.blocks[fromTime(key)] = b
	return nil
}

// Remove the block from the cache
func (c *TimeCache) Remove(key time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocks, fromTime(key))
}

// Get the block from the cache
func (c *TimeCache) Get(key time.Time) (block.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blocks[fromTime(key)]
	return b, ok
}

// MultiGet retrieves multiple blocks from the cache at once
func (c *TimeCache) MultiGet(keys []time.Time) []block.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	blks := make([]block.Block, len(keys))
	for i, key := range keys {
		b, ok := c.blocks[fromTime(key)]
		if ok {
			blks[i] = b
		}
	}

	return blks
}

// MarkProcessed is used to mark a block as processed
func (c *TimeCache) MarkProcessed(keys []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		c.processedBlocks[fromTime(key)] = true
	}
}

// Processed returns all processed block times from the cache
func (c *TimeCache) Processed() map[time.Time]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := make(map[time.Time]bool)
	for k, v := range c.processedBlocks {
		removed[k.toTime()] = v
	}

	return removed
}
