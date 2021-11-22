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
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Type defines the type of repair to run.
type Type uint

const (
	// DefaultRepair will compare node's integrity to other replicas and then repair blocks as required.
	DefaultRepair Type = iota
	// OnlyCompareRepair will compare node's integrity to other replicas without repairing blocks,
	// this is useful for looking at the metrics emitted by the comparison.
	OnlyCompareRepair
)

var validTypes = []Type{
	DefaultRepair,
	OnlyCompareRepair,
}

// MarshalYAML returns the YAML representation of the repair type.
func (t Type) MarshalYAML() (interface{}, error) {
	return t.String(), nil
}

// UnmarshalYAML unmarshals an Type into a valid type from string.
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	// If unspecified, use default mode.
	if str == "" {
		*t = DefaultRepair
		return nil
	}

	for _, valid := range validTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
	}
	return fmt.Errorf("invalid repair Type '%s' valid types are: %s",
		str, validTypes)
}

// String returns the bootstrap mode as a string
func (t Type) String() string {
	switch t {
	case DefaultRepair:
		return "default"
	case OnlyCompareRepair:
		return "only_compare"
	default:
		return "unknown"
	}
}

// Strategy defines the repair strategy.
type Strategy uint

const (
	// DefaultStrategy will compare iterating backwards then on repairing a
	// block needing repair it will restart from the latest block start and
	// work backwards again.
	// This strategy is best at keeping most recent data repaired as quickly
	// as possible but when turning on repair for the first time in a cluster
	// you may want to do run repairs in full sweep for a while first.
	DefaultStrategy Strategy = iota
	// FullSweepStrategy will compare iterating backwards and repairing
	// blocks needing repair until reaching the end of retention and then only
	// once reaching the end of retention to repair does the repair restart
	// evaluating blocks from the most recent block starts again.
	// This mode may be more ideal in clusters that have never had repair
	// enabled to ensure that historical data gets repaired at least once on
	// a full sweep before switching back to the default strategy.
	FullSweepStrategy
)

var validStrategies = []Strategy{
	DefaultStrategy,
	FullSweepStrategy,
}

// MarshalYAML returns the YAML representation of the repair strategy.
func (t Strategy) MarshalYAML() (interface{}, error) {
	return t.String(), nil
}

// UnmarshalYAML unmarshals an Type into a valid type from string.
func (t *Strategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	// If unspecified, use default mode.
	if str == "" {
		*t = DefaultStrategy
		return nil
	}

	for _, valid := range validStrategies {
		if str == valid.String() {
			*t = valid
			return nil
		}
	}
	return fmt.Errorf("invalid repair Strategy '%s' valid types are: %s",
		str, validStrategies)
}

// String returns the bootstrap mode as a string
func (t Strategy) String() string {
	switch t {
	case DefaultStrategy:
		return "default"
	case FullSweepStrategy:
		return "full_sweep"
	default:
		return "unknown"
	}
}

// ReplicaMetadataSlice captures a slice of block.ReplicaMetadata.
type ReplicaMetadataSlice interface {
	// Add adds the metadata to the slice.
	Add(metadata block.ReplicaMetadata)

	// Metadata returns the metadata slice.
	Metadata() []block.ReplicaMetadata

	// Reset resets the metadata slice.
	Reset()

	// Close performs cleanup.
	Close()
}

// ReplicaMetadataSlicePool provides a pool for block.ReplicaMetadata slices.
type ReplicaMetadataSlicePool interface {
	// Get returns a ReplicaMetadata slice.
	Get() ReplicaMetadataSlice

	// Put puts a ReplicaMetadata slice back to pool.
	Put(m ReplicaMetadataSlice)
}

// ReplicaBlockMetadata captures the block metadata from hosts in a shard replica set.
type ReplicaBlockMetadata interface {
	// Start is the start time of a block.
	Start() xtime.UnixNano

	// Metadata returns the metadata from all hosts.
	Metadata() []block.ReplicaMetadata

	// Add adds a metadata from a host.
	Add(metadata block.ReplicaMetadata)

	// Close performs cleanup.
	Close()
}

// ReplicaBlocksMetadata captures the blocks metadata from hosts in a shard replica set.
type ReplicaBlocksMetadata interface {
	// NumBlocks returns the total number of blocks.
	NumBlocks() int64

	// Blocks returns the blocks metadata.
	Blocks() map[xtime.UnixNano]ReplicaBlockMetadata

	// Add adds a block metadata.
	Add(block ReplicaBlockMetadata)

	// GetOrAdd returns the blocks metadata for a start time, creating one if it doesn't exist.
	GetOrAdd(start xtime.UnixNano, p ReplicaMetadataSlicePool) ReplicaBlockMetadata

	// Close performs cleanup.
	Close()
}

// ReplicaSeriesMetadata captures the metadata for a list of series from hosts
// in a shard replica set.
type ReplicaSeriesMetadata interface {
	// NumSeries returns the total number of series.
	NumSeries() int64

	// NumBlocks returns the total number of blocks.
	NumBlocks() int64

	// Series returns the series metadata.
	Series() *Map

	// GetOrAdd returns the series metadata for an id, creating one if it doesn't exist.
	GetOrAdd(id ident.ID) ReplicaBlocksMetadata

	// Close performs cleanup.
	Close()
}

// ReplicaSeriesBlocksMetadata represents series metadata and an associated ID.
type ReplicaSeriesBlocksMetadata struct {
	ID       ident.ID
	Metadata ReplicaBlocksMetadata
}

// ReplicaMetadataComparer compares metadata from hosts in a replica set.
type ReplicaMetadataComparer interface {
	// AddLocalMetadata adds metadata from local host.
	AddLocalMetadata(localIter block.FilteredBlocksMetadataIter) error

	// AddPeerMetadata adds metadata from peers.
	AddPeerMetadata(peerIter client.PeerBlockMetadataIter) error

	// Compare returns the metadata differences between local host and peers.
	Compare() MetadataComparisonResult

	// Finalize performs cleanup during close.
	Finalize()
}

// MetadataComparisonResult captures metadata comparison results.
type MetadataComparisonResult struct {
	// NumSeries returns the total number of series.
	NumSeries int64

	// NumBlocks returns the total number of blocks.
	NumBlocks int64

	// SizeResult returns the size differences.
	SizeDifferences ReplicaSeriesMetadata

	// ChecksumDifferences returns the checksum differences.
	ChecksumDifferences ReplicaSeriesMetadata

	// PeerMetadataComparisonResults the results comparative to each peer.
	PeerMetadataComparisonResults PeerMetadataComparisonResults
}

// PeerMetadataComparisonResult captures metadata comparison results
// relative to the local origin node.
type PeerMetadataComparisonResult struct {
	// ID is the peer ID.
	ID string

	// ComparedBlocks returns the total number of blocks.
	ComparedBlocks int64

	// ComparedDifferingBlocks returns the number of differing blocks (mismatch + missing + extra).
	ComparedDifferingBlocks int64

	// ComparedMismatchBlocks returns the number of mismatching blocks (either size or checksum).
	ComparedMismatchBlocks int64

	// ComparedMissingBlocks returns the number of missing blocks.
	ComparedMissingBlocks int64

	// ComparedExtraBlocks returns the number of extra blocks.
	ComparedExtraBlocks int64
}

// PeerMetadataComparisonResults is a slice of PeerMetadataComparisonResult.
type PeerMetadataComparisonResults []PeerMetadataComparisonResult

// Aggregate returns an aggregate result of the PeerMetadataComparisonResults.
func (r PeerMetadataComparisonResults) Aggregate() AggregatePeerMetadataComparisonResult {
	var result AggregatePeerMetadataComparisonResult
	for _, elem := range r {
		result.ComparedBlocks += elem.ComparedBlocks
		result.ComparedDifferingBlocks += elem.ComparedDifferingBlocks
		result.ComparedMismatchBlocks += elem.ComparedMismatchBlocks
		result.ComparedMissingBlocks += elem.ComparedMissingBlocks
		result.ComparedExtraBlocks += elem.ComparedExtraBlocks
	}
	if result.ComparedBlocks <= 0 {
		// Do not divide by zero and end up with a struct that cannot be JSON serialized.
		return result
	}
	result.ComparedDifferingPercent = float64(result.ComparedDifferingBlocks) / float64(result.ComparedBlocks)
	return result
}

// AggregatePeerMetadataComparisonResult captures an aggregate metadata comparison
// result of all peers relative to the local origin node.
type AggregatePeerMetadataComparisonResult struct {
	// ComparedDifferingPercent is the percent of blocks that mismatched from peers to local origin.
	ComparedDifferingPercent float64

	// ComparedBlocks returns the total number of blocks compared.
	ComparedBlocks int64

	// ComparedDifferingBlocks returns the number of differing blocks (mismatch + missing + extra).
	ComparedDifferingBlocks int64

	// ComparedMismatchBlocks returns the number of mismatching blocks (either size or checksum).
	ComparedMismatchBlocks int64

	// ComparedMissingBlocks returns the number of missing blocks.
	ComparedMissingBlocks int64

	// ComparedExtraBlocks returns the number of extra blocks.
	ComparedExtraBlocks int64
}

// Options are the repair options.
type Options interface {
	// SetType sets the type of repair to run.
	SetType(value Type) Options

	// Type returns the type of repair to run.
	Type() Type

	// SetStrategy sets the repair strategy.
	SetStrategy(value Strategy) Options

	// Strategy returns the repair strategy.
	Strategy() Strategy

	// SetForce sets whether to force repairs to run for all namespaces.
	SetForce(value bool) Options

	// Force returns whether to force repairs to run for all namespaces.
	Force() bool

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
