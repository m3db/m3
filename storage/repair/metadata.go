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
)

type replicaBlockMetadata struct {
	start    time.Time
	metadata []HostBlockMetadata
}

// NewReplicaBlockMetadata creates a new replica block metadata
func NewReplicaBlockMetadata(start time.Time) ReplicaBlockMetadata {
	return &replicaBlockMetadata{start: start}
}

func (m *replicaBlockMetadata) Start() time.Time              { return m.start }
func (m *replicaBlockMetadata) Metadata() []HostBlockMetadata { return m.metadata }
func (m *replicaBlockMetadata) Add(metadata HostBlockMetadata) {
	m.metadata = append(m.metadata, metadata)
}

type replicaBlocksMetadata map[time.Time]ReplicaBlockMetadata

func newReplicaBlocksMetadata() ReplicaBlocksMetadata {
	return make(replicaBlocksMetadata)
}

func (m replicaBlocksMetadata) Blocks() map[time.Time]ReplicaBlockMetadata { return m }
func (m replicaBlocksMetadata) Add(block ReplicaBlockMetadata)             { m[block.Start()] = block }

func (m replicaBlocksMetadata) GetOrAdd(start time.Time) ReplicaBlockMetadata {
	block, exists := m[start]
	if exists {
		return block
	}
	block = NewReplicaBlockMetadata(start)
	m[start] = block
	return block
}

type replicaSeriesMetadata map[string]ReplicaBlocksMetadata

func newReplicaSeriesMetadata() ReplicaSeriesMetadata {
	return make(replicaSeriesMetadata)
}

func (m replicaSeriesMetadata) Series() map[string]ReplicaBlocksMetadata { return m }

func (m replicaSeriesMetadata) GetOrAdd(id string) ReplicaBlocksMetadata {
	blocks, exists := m[id]
	if exists {
		return blocks
	}
	blocks = newReplicaBlocksMetadata()
	m[id] = blocks
	return blocks
}

type replicaMetadataComparer struct {
	replicas int
	metadata ReplicaSeriesMetadata
}

// NewReplicaMetadataComparer creates a new replica metadata comparer
func NewReplicaMetadataComparer(replicas int) ReplicaMetadataComparer {
	return replicaMetadataComparer{
		replicas: replicas,
		metadata: newReplicaSeriesMetadata(),
	}
}

func (m replicaMetadataComparer) AddLocalMetadata(origin topology.Host, localIter block.FilteredBlocksMetadataIter) {
	for localIter.Next() {
		id, block := localIter.Current()
		blocks := m.metadata.GetOrAdd(id)
		blocks.GetOrAdd(block.Start()).Add(HostBlockMetadata{
			Host:     origin,
			Size:     block.Size(),
			Checksum: block.Checksum(),
		})
	}
}

func (m replicaMetadataComparer) AddPeerMetadata(peerIter client.PeerBlocksMetadataIter) error {
	for peerIter.Next() {
		peer, peerBlocks := peerIter.Current()
		blocks := m.metadata.GetOrAdd(peerBlocks.ID())
		for _, pb := range peerBlocks.Blocks() {
			blocks.GetOrAdd(pb.Start()).Add(HostBlockMetadata{
				Host:     peer,
				Size:     pb.Size(),
				Checksum: pb.Checksum(),
			})
		}
	}

	return peerIter.Err()
}

func (m replicaMetadataComparer) Compare() MetadataComparisonResult {
	var (
		numSeries    int64
		numBlocks    int64
		sizeDiff     = newReplicaSeriesMetadata()
		checkSumDiff = newReplicaSeriesMetadata()
	)

	allSeries := m.metadata.Series()
	numSeries = int64(len(allSeries))
	for id, series := range allSeries {
		blocks := series.Blocks()
		numBlocks += int64(len(blocks))
		for _, b := range blocks {
			bm := b.Metadata()

			var (
				numHostsWithSize     int
				sizeVal              int64
				sameSize             = true
				firstSize            = true
				numHostsWithChecksum int
				checksumVal          uint32
				sameChecksum         = true
				firstChecksum        = true
			)

			for _, hm := range bm {
				// Check size
				if hm.Size != 0 {
					numHostsWithSize++
					if firstSize {
						sizeVal = hm.Size
						firstSize = false
					} else if hm.Size != sizeVal {
						sameSize = false
					}
				}

				// Check checksum
				if hm.Checksum != nil {
					numHostsWithChecksum++
					if firstChecksum {
						checksumVal = *hm.Checksum
						firstChecksum = false
					} else if *hm.Checksum != checksumVal {
						sameChecksum = false
					}
				}
			}

			// If only a subset of hosts in the replica set have sizes, or the sizes differ,
			// we record this block
			if !(numHostsWithSize == m.replicas && sameSize) {
				sizeDiff.GetOrAdd(id).Add(b)
			}

			// If only a subset of hosts in the replica set have checksums, or the checksums
			// differ, we record this block
			if !(numHostsWithChecksum == m.replicas && sameChecksum) {
				checkSumDiff.GetOrAdd(id).Add(b)
			}
		}
	}

	return MetadataComparisonResult{
		NumSeries:           numSeries,
		NumBlocks:           numBlocks,
		SizeDifferences:     sizeDiff,
		ChecksumDifferences: checkSumDiff,
	}
}
