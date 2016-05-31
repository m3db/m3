package bootstrap

import (
	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/storage"
)

type shardResult struct {
	blocks map[string]memtsdb.DatabaseSeriesBlocks
	dbOpts memtsdb.DatabaseOptions
}

// NewShardResult creates a new TSMap instance.
func NewShardResult(dbOpts memtsdb.DatabaseOptions) memtsdb.ShardResult {
	return &shardResult{
		blocks: make(map[string]memtsdb.DatabaseSeriesBlocks),
		dbOpts: dbOpts,
	}
}

// IsEmpty returns whether the result is empty.
func (sr *shardResult) IsEmpty() bool {
	return len(sr.blocks) == 0
}

// AddBlock adds a data block.
func (sr *shardResult) AddBlock(id string, block memtsdb.DatabaseBlock) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = storage.NewDatabaseSeriesBlocks(sr.dbOpts)
		sr.blocks[id] = curSeries
	}
	curSeries.AddBlock(block)
}

// AddSeries adds a single series.
func (sr *shardResult) AddSeries(id string, rawSeries memtsdb.DatabaseSeriesBlocks) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = storage.NewDatabaseSeriesBlocks(sr.dbOpts)
		sr.blocks[id] = curSeries
	}
	curSeries.AddSeries(rawSeries)
}

// AddResult adds a shard result.
func (sr *shardResult) AddResult(other memtsdb.ShardResult) {
	otherSeries := other.GetAllSeries()
	for id, rawSeries := range otherSeries {
		sr.AddSeries(id, rawSeries)
	}
}

// GetAllSeries returns all series in the map.
func (sr *shardResult) GetAllSeries() map[string]memtsdb.DatabaseSeriesBlocks {
	return sr.blocks
}
