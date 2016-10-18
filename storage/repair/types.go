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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
)

// HostBlockMetadata contains a host along with block metadata from that host
type HostBlockMetadata struct {
	Host     topology.Host
	Size     int64
	Checksum *uint32
}

// HostBlockMetadataSlice captures a slice of hostBlockMetadata
type HostBlockMetadataSlice interface {
	// Add adds the metadata to the slice
	Add(metadata HostBlockMetadata)

	// Metadata returns the metadata slice
	Metadata() []HostBlockMetadata

	// Reset resets the metadata slice
	Reset()

	// Close performs cleanup
	Close()
}

// HostBlockMetadataSlicePool provides a pool for hostBlockMetadata slices
type HostBlockMetadataSlicePool interface {
	// Get returns a hostBlockMetadata slice
	Get() HostBlockMetadataSlice

	// Put puts a hostBlockMetadata slice back to pool
	Put(m HostBlockMetadataSlice)
}

// ReplicaBlockMetadata captures the block metadata from hosts in a shard replica set
type ReplicaBlockMetadata interface {
	// Start is the start time of a block
	Start() time.Time

	// Metadata returns the metadata from all hosts
	Metadata() []HostBlockMetadata

	// Add adds a metadata from a host
	Add(metadata HostBlockMetadata)

	// Close performs cleanup
	Close()
}

// ReplicaBlocksMetadata captures the blocks metadata from hosts in a shard replica set
type ReplicaBlocksMetadata interface {
	// NumBlocks returns the total number of blocks
	NumBlocks() int64

	// Blocks returns the blocks metadata
	Blocks() map[time.Time]ReplicaBlockMetadata

	// Add adds a block metadata
	Add(block ReplicaBlockMetadata)

	// GetOrAdd returns the blocks metadata for a start time, creating one if it doesn't exist
	GetOrAdd(start time.Time, p HostBlockMetadataSlicePool) ReplicaBlockMetadata

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
	Series() map[ts.Hash]ReplicaBlocksMetadataWrapper

	// GetOrAdd returns the series metadata for an id, creating one if it doesn't exist
	GetOrAdd(id ts.ID) ReplicaBlocksMetadata

	// Close performs cleanup
	Close()
}

// ReplicaBlocksMetadataWrapper represents series metadata and an associated ID.
type ReplicaBlocksMetadataWrapper struct {
	ID       ts.ID
	Metadata ReplicaBlocksMetadata
}

// ReplicaMetadataComparer compares metadata from hosts in a replica set
type ReplicaMetadataComparer interface {
	// AddLocalMetadata adds metadata from local host
	AddLocalMetadata(origin topology.Host, localIter block.FilteredBlocksMetadataIter)

	// AddPeerMetadata adds metadata from peers
	AddPeerMetadata(peerIter client.PeerBlocksMetadataIter) error

	// Compare returns the metadata differences between local host and peers
	Compare() MetadataComparisonResult

	// OnClose performs cleanup during close
	OnClose()
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
	// SetAdminClient sets the admin client
	SetAdminClient(value client.AdminClient) Options

	// AdminClient returns the admin client
	AdminClient() client.AdminClient

	// SetRepairShardConcurrency sets the concurrency in which to repair shards with
	SetRepairShardConcurrency(value int) Options

	// RepairShardConcurrency returns the concurrency in which to repair shards with
	RepairShardConcurrency() int

	// SetRepairInterval sets the repair interval
	SetRepairInterval(value time.Duration) Options

	// RepairInterval returns the repair interval
	RepairInterval() time.Duration

	// SetRepairTimeOffset sets the repair time offset
	SetRepairTimeOffset(value time.Duration) Options

	// RepairTimeOffset returns the repair time offset
	RepairTimeOffset() time.Duration

	// SetRepairJitter sets the repair time jitter
	SetRepairTimeJitter(value time.Duration) Options

	// RepairTimeJitter returns the repair time jitter
	RepairTimeJitter() time.Duration

	// SetRepairCheckInterval sets the repair check interval
	SetRepairCheckInterval(value time.Duration) Options

	// RepairCheckInterval returns the repair check interval
	RepairCheckInterval() time.Duration

	// SetRepairThrottle sets the repair throttle
	SetRepairThrottle(value time.Duration) Options

	// RepairThrottle returns the repair throttle
	RepairThrottle() time.Duration

	// SetRepairMaxRetries sets the max number of retries for a block start
	SetRepairMaxRetries(value int) Options

	// MaxRepairRetries returns the max number of retries for a block start
	RepairMaxRetries() int

	// SetHostBlockMetadataSlicePool sets the hostBlockMetadataSlice pool
	SetHostBlockMetadataSlicePool(value HostBlockMetadataSlicePool) Options

	// HostBlockMetadataSlicePool returns the hostBlockMetadataSlice pool
	HostBlockMetadataSlicePool() HostBlockMetadataSlicePool

	// Validate checks if the options are valid
	Validate() error
}
