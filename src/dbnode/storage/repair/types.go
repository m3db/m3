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

package repair

import (
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// ReplicaMetadataSlice captures a slice of block.ReplicaMetadata
type ReplicaMetadataSlice interface {
	// Add adds the metadata to the slice
	Add(metadata block.ReplicaMetadata)

	// Metadata returns the metadata slice
	Metadata() []block.ReplicaMetadata

	// Reset resets the metadata slice
	Reset()

	// Close performs cleanup
	Close()
}

// ReplicaMetadataSlicePool provides a pool for block.ReplicaMetadata slices
type ReplicaMetadataSlicePool interface {
	// Get returns a ReplicaMetadata slice
	Get() ReplicaMetadataSlice

	// Put puts a ReplicaMetadata slice back to pool
	Put(m ReplicaMetadataSlice)
}

// ReplicaBlockMetadata captures the block metadata from hosts in a shard replica set
type ReplicaBlockMetadata interface {
	// Start is the start time of a block
	Start() time.Time

	// Metadata returns the metadata from all hosts
	Metadata() []block.ReplicaMetadata

	// Add adds a metadata from a host
	Add(metadata block.ReplicaMetadata)

	// Close performs cleanup
	Close()
}

// ReplicaBlocksMetadata captures the blocks metadata from hosts in a shard replica set
type ReplicaBlocksMetadata interface {
	// NumBlocks returns the total number of blocks
	NumBlocks() int64

	// Blocks returns the blocks metadata
	Blocks() map[xtime.UnixNano]ReplicaBlockMetadata

	// Add adds a block metadata
	Add(block ReplicaBlockMetadata)

	// GetOrAdd returns the blocks metadata for a start time, creating one if it doesn't exist
	GetOrAdd(start time.Time, p ReplicaMetadataSlicePool) ReplicaBlockMetadata

	// Close performs cleanup
	Close()
}

// ReplicaSeriesMetadata captures the metadata for a list of series from hosts in a shard replica set
type ReplicaSeriesMetadata interface {
	// NumSeries returns the total number of series
	NumSeries() int64

	// NumBlocks returns the total number of blocks
	NumBlocks() int64

	// Series returns the series metadata
	Series() *Map

	// GetOrAdd returns the series metadata for an id, creating one if it doesn't exist
	GetOrAdd(id ident.ID) ReplicaBlocksMetadata

	// Close performs cleanup
	Close()
}

// ReplicaSeriesBlocksMetadata represents series metadata and an associated ID.
type ReplicaSeriesBlocksMetadata struct {
	ID       ident.ID
	Metadata ReplicaBlocksMetadata
}

// ReplicaMetadataComparer compares metadata from hosts in a replica set
type ReplicaMetadataComparer interface {
	// AddLocalMetadata adds metadata from local host
	AddLocalMetadata(localIter block.FilteredBlocksMetadataIter) error

	// AddPeerMetadata adds metadata from peers
	AddPeerMetadata(peerIter client.PeerBlockMetadataIter) error

	// Compare returns the metadata differences between local host and peers
	Compare() MetadataComparisonResult

	// Finalize performs cleanup during close
	Finalize()
}

// MetadataComparisonResult captures metadata comparison results
type MetadataComparisonResult struct {
	// NumSeries returns the total number of series
	NumSeries int64

	// NumBlocks returns the total number of blocks
	NumBlocks int64

	// SizeResult returns the size differences
	SizeDifferences ReplicaSeriesMetadata

	// ChecksumDifferences returns the checksum differences
	ChecksumDifferences ReplicaSeriesMetadata
}

// Options are the repair options
type Options interface {
	// SetAdminClient sets the admin client.
	SetAdminClients(value []client.AdminClient) Options

	// AdminClient returns the admin client.
	AdminClients() []client.AdminClient

	// SetRepairConsistencyLevel sets the repair read level consistency
	// for which to repair shards with.
	SetRepairConsistencyLevel(value topology.ReadConsistencyLevel) Options

	// RepairConsistencyLevel returns the repair read level consistency
	// for which to repair shards with.
	RepairConsistencyLevel() topology.ReadConsistencyLevel

	// SetRepairShardConcurrency sets the concurrency in which to repair shards with.
	SetRepairShardConcurrency(value int) Options

	// RepairShardConcurrency returns the concurrency in which to repair shards with.
	RepairShardConcurrency() int

	// SetRepairCheckInterval sets the repair check interval.
	SetRepairCheckInterval(value time.Duration) Options

	// RepairCheckInterval returns the repair check interval.
	RepairCheckInterval() time.Duration

	// SetRepairThrottle sets the repair throttle.
	SetRepairThrottle(value time.Duration) Options

	// RepairThrottle returns the repair throttle.
	RepairThrottle() time.Duration

	// SetReplicaMetadataSlicePool sets the replicaMetadataSlice pool.
	SetReplicaMetadataSlicePool(value ReplicaMetadataSlicePool) Options

	// ReplicaMetadataSlicePool returns the replicaMetadataSlice pool.
	ReplicaMetadataSlicePool() ReplicaMetadataSlicePool

	// SetResultOptions sets the result options.
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the result options.
	ResultOptions() result.Options

	// SetDebugShadowComparisonsEnabled sets whether debug shadow comparisons are enabled.
	SetDebugShadowComparisonsEnabled(value bool) Options

	// DebugShadowComparisonsEnabled returns whether debug shadow comparisons are enabled.
	DebugShadowComparisonsEnabled() bool

	// SetDebugShadowComparisonsPercentage sets the debug shadow comparisons percentage.
	SetDebugShadowComparisonsPercentage(value float64) Options

	// DebugShadowComparisonsPercentage returns the debug shadow comparisons percentage.
	DebugShadowComparisonsPercentage() float64

	// Validate checks if the options are valid.
	Validate() error
}
