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

package m3db

import (
	"time"

	xtime "github.com/m3db/m3db/x/time"
)

// NewDatabaseBlockFn creates a new database block.
type NewDatabaseBlockFn func() DatabaseBlock

// DatabaseBlock represents a data block.
type DatabaseBlock interface {
	StartTime() time.Time
	Write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error
	Stream() SegmentReader
	Close()
}

// DatabaseSeriesBlocks represents a collection of data blocks.
type DatabaseSeriesBlocks interface {

	// Number of blocks contained in the collection.
	Len() int

	// AddBlock adds a data block.
	AddBlock(block DatabaseBlock)

	// AddSeries adds a raw series.
	AddSeries(other DatabaseSeriesBlocks)

	// GetMinTime returns the min time of the blocks contained.
	GetMinTime() time.Time

	// GetMaxTime returns the max time of the blocks contained.
	GetMaxTime() time.Time

	// GetBlockAt returns the block at a given time if any.
	GetBlockAt(t time.Time) (DatabaseBlock, bool)

	// GetBlockAt returns the block at a given time, add it if it doesn't exist.
	GetBlockOrAdd(t time.Time) DatabaseBlock

	// GetAllBlocks returns all the blocks in the series.
	GetAllBlocks() map[time.Time]DatabaseBlock

	// RemoveBlockAt removes the block at a given time if any.
	RemoveBlockAt(t time.Time)
}
