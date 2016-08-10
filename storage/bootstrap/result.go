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

package bootstrap

import (
	"github.com/m3db/m3db/storage/block"
)

type shardResult struct {
	opts   Options
	blocks map[string]block.DatabaseSeriesBlocks
}

// NewShardResult creates a new TSMap instance.
func NewShardResult(opts Options) ShardResult {
	return &shardResult{
		opts:   opts,
		blocks: make(map[string]block.DatabaseSeriesBlocks),
	}
}

// IsEmpty returns whether the result is empty.
func (sr *shardResult) IsEmpty() bool {
	return len(sr.blocks) == 0
}

// AddBlock adds a data block.
func (sr *shardResult) AddBlock(id string, b block.DatabaseBlock) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = block.NewDatabaseSeriesBlocks(sr.opts.GetDatabaseBlockOptions())
		sr.blocks[id] = curSeries
	}
	curSeries.AddBlock(b)
}

// AddSeries adds a single series.
func (sr *shardResult) AddSeries(id string, rawSeries block.DatabaseSeriesBlocks) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = block.NewDatabaseSeriesBlocks(sr.opts.GetDatabaseBlockOptions())
		sr.blocks[id] = curSeries
	}
	curSeries.AddSeries(rawSeries)
}

// AddResult adds a shard result.
func (sr *shardResult) AddResult(other ShardResult) {
	if other == nil {
		return
	}
	otherSeries := other.GetAllSeries()
	for id, rawSeries := range otherSeries {
		sr.AddSeries(id, rawSeries)
	}
}

// RemoveSeries removes a single series of blocks.
func (sr *shardResult) RemoveSeries(id string) {
	delete(sr.blocks, id)
}

// GetAllSeries returns all series in the map.
func (sr *shardResult) GetAllSeries() map[string]block.DatabaseSeriesBlocks {
	return sr.blocks
}

// Close closes a shard result.
func (sr *shardResult) Close() {
	for _, series := range sr.blocks {
		series.Close()
	}
}
