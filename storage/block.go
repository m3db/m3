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

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

type dbBlock struct {
	opts    m3db.DatabaseOptions
	start   time.Time
	encoder m3db.Encoder
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, encoder m3db.Encoder, opts m3db.DatabaseOptions) m3db.DatabaseBlock {
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
	return b.encoder.Encode(m3db.Datapoint{Timestamp: timestamp, Value: value}, unit, annotation)
}

func (b *dbBlock) Stream() m3db.SegmentReader {
	return b.encoder.Stream()
}

func (b *dbBlock) Close() {
	// This will return the encoder to the pool
	b.encoder.Close()
}

type databaseSeriesBlocks struct {
	elems  map[time.Time]m3db.DatabaseBlock
	min    time.Time
	max    time.Time
	dbOpts m3db.DatabaseOptions
}

// NewDatabaseSeriesBlocks creates a databaseSeriesBlocks instance.
func NewDatabaseSeriesBlocks(dbOpts m3db.DatabaseOptions) m3db.DatabaseSeriesBlocks {
	return &databaseSeriesBlocks{
		elems:  make(map[time.Time]m3db.DatabaseBlock),
		dbOpts: dbOpts,
	}
}

func (dbb *databaseSeriesBlocks) Len() int {
	return len(dbb.elems)
}

func (dbb *databaseSeriesBlocks) AddBlock(block m3db.DatabaseBlock) {
	start := block.StartTime()
	if dbb.min.Equal(timeZero) || start.Before(dbb.min) {
		dbb.min = start
	}
	if dbb.max.Equal(timeZero) || start.After(dbb.max) {
		dbb.max = start
	}
	dbb.elems[start] = block
}

func (dbb *databaseSeriesBlocks) AddSeries(other m3db.DatabaseSeriesBlocks) {
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

func (dbb *databaseSeriesBlocks) GetBlockAt(t time.Time) (m3db.DatabaseBlock, bool) {
	b, ok := dbb.elems[t]
	return b, ok
}

func (dbb *databaseSeriesBlocks) GetBlockOrAdd(t time.Time) m3db.DatabaseBlock {
	b, ok := dbb.elems[t]
	if ok {
		return b
	}
	newBlock := NewDatabaseBlock(t, nil, dbb.dbOpts)
	dbb.AddBlock(newBlock)
	return newBlock
}

func (dbb *databaseSeriesBlocks) GetAllBlocks() map[time.Time]m3db.DatabaseBlock {
	return dbb.elems
}
