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

package storage

import (
	"time"

	"github.com/m3db/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

type dbBlock struct {
	opts    memtsdb.DatabaseOptions
	start   time.Time
	encoder memtsdb.Encoder
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, encoder memtsdb.Encoder, opts memtsdb.DatabaseOptions) memtsdb.DatabaseBlock {
	return &dbBlock{
		opts:    opts,
		start:   start,
		encoder: encoder,
	}
}

func (b *dbBlock) StartTime() time.Time {
	return b.start
}

func (b *dbBlock) Write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return b.encoder.Encode(memtsdb.Datapoint{Timestamp: timestamp, Value: value}, unit, annotation)
}

func (b *dbBlock) Stream() memtsdb.SegmentReader {
	return b.encoder.Stream()
}

func (b *dbBlock) Close() {
	// This will return the encoder to the pool
	b.encoder.Close()
}

type databaseSeriesBlocks struct {
	elems  map[time.Time]memtsdb.DatabaseBlock
	min    time.Time
	max    time.Time
	dbOpts memtsdb.DatabaseOptions
}

// NewDatabaseSeriesBlocks creates a databaseSeriesBlocks instance.
func NewDatabaseSeriesBlocks(dbOpts memtsdb.DatabaseOptions) memtsdb.DatabaseSeriesBlocks {
	return &databaseSeriesBlocks{
		elems:  make(map[time.Time]memtsdb.DatabaseBlock),
		dbOpts: dbOpts,
	}
}

func (dbb *databaseSeriesBlocks) AddBlock(block memtsdb.DatabaseBlock) {
	start := block.StartTime()
	if dbb.min.Equal(timeZero) || start.Before(dbb.min) {
		dbb.min = start
	}
	if dbb.max.Equal(timeZero) || start.After(dbb.max) {
		dbb.max = start
	}
	dbb.elems[start] = block
}

func (dbb *databaseSeriesBlocks) AddSeries(other memtsdb.DatabaseSeriesBlocks) {
	if other == nil {
		return
	}
	blocks := other.GetAllBlocks()
	for _, b := range blocks {
		dbb.AddBlock(b)
	}
}

// GetMinTime returns the min time of the blocks contained.
func (dbb *databaseSeriesBlocks) GetMinTime() time.Time {
	return dbb.min
}

// GetMaxTime returns the max time of the blocks contained.
func (dbb *databaseSeriesBlocks) GetMaxTime() time.Time {
	return dbb.max
}

func (dbb *databaseSeriesBlocks) GetBlockAt(t time.Time) (memtsdb.DatabaseBlock, bool) {
	b, ok := dbb.elems[t]
	return b, ok
}

func (dbb *databaseSeriesBlocks) GetBlockOrAdd(t time.Time) memtsdb.DatabaseBlock {
	b, ok := dbb.elems[t]
	if ok {
		return b
	}
	newBlock := NewDatabaseBlock(t, nil, dbb.dbOpts)
	dbb.AddBlock(newBlock)
	return newBlock
}

func (dbb *databaseSeriesBlocks) GetAllBlocks() map[time.Time]memtsdb.DatabaseBlock {
	return dbb.elems
}
