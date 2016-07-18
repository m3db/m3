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
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/storage"
)

type shardResult struct {
	blocks map[string]m3db.DatabaseSeriesBlocks
	dbOpts m3db.DatabaseOptions
}

// NewShardResult creates a new TSMap instance.
func NewShardResult(dbOpts m3db.DatabaseOptions) m3db.ShardResult {
	return &shardResult{
		blocks: make(map[string]m3db.DatabaseSeriesBlocks),
		dbOpts: dbOpts,
	}
}

// IsEmpty returns whether the result is empty.
func (sr *shardResult) IsEmpty() bool {
	return len(sr.blocks) == 0
}

// AddBlock adds a data block.
func (sr *shardResult) AddBlock(id string, block m3db.DatabaseBlock) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = storage.NewDatabaseSeriesBlocks(sr.dbOpts)
		sr.blocks[id] = curSeries
	}
	curSeries.AddBlock(block)
}

// AddSeries adds a single series.
func (sr *shardResult) AddSeries(id string, rawSeries m3db.DatabaseSeriesBlocks) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = storage.NewDatabaseSeriesBlocks(sr.dbOpts)
		sr.blocks[id] = curSeries
	}
	curSeries.AddSeries(rawSeries)
}

// AddResult adds a shard result.
func (sr *shardResult) AddResult(other m3db.ShardResult) {
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
func (sr *shardResult) GetAllSeries() map[string]m3db.DatabaseSeriesBlocks {
	return sr.blocks
}

// Close closes a shard result.
func (sr *shardResult) Close() {
	for _, series := range sr.blocks {
		series.Close()
	}
}
