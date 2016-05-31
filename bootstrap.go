package memtsdb

import (
	"time"

	xtime "code.uber.internal/infra/memtsdb/x/time"
)

// NewBootstrapFn creates a new bootstrap processor.
type NewBootstrapFn func() Bootstrap

// ShardResult returns the bootstrap result for a shard.
type ShardResult interface {

	// IsEmpty returns whether the result is empty.
	IsEmpty() bool

	// AddBlock adds a data block.
	AddBlock(id string, block DatabaseBlock)

	// AddSeries adds a single series of blocks.
	AddSeries(id string, rawSeries DatabaseSeriesBlocks)

	// AddResult adds a shard result.
	AddResult(other ShardResult)

	// GetAllSeries returns all series of blocks.
	GetAllSeries() map[string]DatabaseSeriesBlocks
}

// Bootstrap represents the bootstrap process.
type Bootstrap interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(writeStart time.Time, shard uint32) (ShardResult, error)
}

// Bootstrapper is the interface for different bootstrapping mechanisms.
type Bootstrapper interface {
	// Bootstrap performs bootstrapping for the given time ranges, returning the bootstrapped
	// series data, the time ranges it's unable to fulfill, and any critical errors during bootstrapping.
	Bootstrap(shard uint32, timeRanges xtime.Ranges) (ShardResult, xtime.Ranges)
}

// Source is the data source for bootstrapping a node.
type Source interface {
	// GetAvailability returns what time ranges are available for a given shard.
	GetAvailability(shard uint32, targetRanges xtime.Ranges) xtime.Ranges

	// ReadData returns raw series for a given shard within certain time ranges,
	// the time ranges it's unable to fulfill, and any critical errors during bootstrapping.
	ReadData(shard uint32, tr xtime.Ranges) (ShardResult, xtime.Ranges)
}
