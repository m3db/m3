package memtsdb

import (
	"time"

	xtime "code.uber.internal/infra/memtsdb/x/time"
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
}
