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
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	defaultReplicaBlocksMetadataCapacity = 1
	defaultReplicaSeriesMetadataCapacity = 4096
)

type replicaMetadataSlice struct {
	metadata []block.ReplicaMetadata
	pool     ReplicaMetadataSlicePool
}

func newReplicaMetadataSlice() ReplicaMetadataSlice {
	return &replicaMetadataSlice{}
}

func newPooledReplicaMetadataSlice(metadata []block.ReplicaMetadata, pool ReplicaMetadataSlicePool) ReplicaMetadataSlice {
	return &replicaMetadataSlice{metadata: metadata, pool: pool}
}

func (s *replicaMetadataSlice) Add(metadata block.ReplicaMetadata) {
	s.metadata = append(s.metadata, metadata)
}

func (s *replicaMetadataSlice) Metadata() []block.ReplicaMetadata {
	return s.metadata
}

func (s *replicaMetadataSlice) Reset() {
	var zeroed block.ReplicaMetadata
	for i := range s.metadata {
		s.metadata[i] = zeroed
	}
	s.metadata = s.metadata[:0]
}

func (s *replicaMetadataSlice) Close() {
	if s.pool != nil {
		s.pool.Put(s)
	}
}

type replicaBlockMetadata struct {
	start    xtime.UnixNano
	metadata ReplicaMetadataSlice
}

// NewReplicaBlockMetadata creates a new replica block metadata
func NewReplicaBlockMetadata(start xtime.UnixNano, p ReplicaMetadataSlice) ReplicaBlockMetadata {
	return replicaBlockMetadata{start: start, metadata: p}
}

func (m replicaBlockMetadata) Start() xtime.UnixNano              { return m.start }
func (m replicaBlockMetadata) Metadata() []block.ReplicaMetadata  { return m.metadata.Metadata() }
func (m replicaBlockMetadata) Add(metadata block.ReplicaMetadata) { m.metadata.Add(metadata) }
func (m replicaBlockMetadata) Close()                             { m.metadata.Close() }

type replicaBlocksMetadata map[xtime.UnixNano]ReplicaBlockMetadata

// NewReplicaBlocksMetadata creates a new replica blocks metadata
func NewReplicaBlocksMetadata() ReplicaBlocksMetadata {
	return make(replicaBlocksMetadata, defaultReplicaBlocksMetadataCapacity)
}

func (m replicaBlocksMetadata) NumBlocks() int64                                { return int64(len(m)) }
func (m replicaBlocksMetadata) Blocks() map[xtime.UnixNano]ReplicaBlockMetadata { return m }
func (m replicaBlocksMetadata) Add(block ReplicaBlockMetadata) {
	m[block.Start()] = block
}

func (m replicaBlocksMetadata) GetOrAdd(start xtime.UnixNano, p ReplicaMetadataSlicePool) ReplicaBlockMetadata {
	block, exists := m[start]
	if exists {
		return block
	}
	block = NewReplicaBlockMetadata(start, p.Get())
	m[start] = block
	return block
}

func (m replicaBlocksMetadata) Close() {
	for _, b := range m {
		b.Close()
	}
}

// NB(xichen): replicaSeriesMetadata is not thread-safe
type replicaSeriesMetadata struct {
	values *Map
}

// NewReplicaSeriesMetadata creates a new replica series metadata
func NewReplicaSeriesMetadata() ReplicaSeriesMetadata {
	return replicaSeriesMetadata{
		values: NewMap(MapOptions{InitialSize: defaultReplicaSeriesMetadataCapacity}),
	}
}

func (m replicaSeriesMetadata) NumSeries() int64 { return int64(m.values.Len()) }
func (m replicaSeriesMetadata) Series() *Map     { return m.values }

func (m replicaSeriesMetadata) NumBlocks() int64 {
	var numBlocks int64
	for _, entry := range m.values.Iter() {
		series := entry.Value()
		numBlocks += series.Metadata.NumBlocks()
	}
	return numBlocks
}

func (m replicaSeriesMetadata) GetOrAdd(id ident.ID) ReplicaBlocksMetadata {
	blocks, exists := m.values.Get(id)
	if exists {
		return blocks.Metadata
	}
	blocks = ReplicaSeriesBlocksMetadata{
		ID:       id,
		Metadata: NewReplicaBlocksMetadata(),
	}
	m.values.Set(id, blocks)
	return blocks.Metadata
}

func (m replicaSeriesMetadata) Close() {
	for _, entry := range m.values.Iter() {
		series := entry.Value()
		series.Metadata.Close()
	}
}

type replicaMetadataComparer struct {
	origin                   topology.Host
	metadata                 ReplicaSeriesMetadata
	replicaMetadataSlicePool ReplicaMetadataSlicePool
}

// NewReplicaMetadataComparer creates a new replica metadata comparer
func NewReplicaMetadataComparer(origin topology.Host, opts Options) ReplicaMetadataComparer {
	return replicaMetadataComparer{
		origin:                   origin,
		metadata:                 NewReplicaSeriesMetadata(),
		replicaMetadataSlicePool: opts.ReplicaMetadataSlicePool(),
	}
}

func (m replicaMetadataComparer) AddLocalMetadata(localIter block.FilteredBlocksMetadataIter) error {
	for localIter.Next() {
		id, localBlock := localIter.Current()
		blocks := m.metadata.GetOrAdd(id)
		blocks.GetOrAdd(localBlock.Start, m.replicaMetadataSlicePool).Add(block.ReplicaMetadata{
			Host:     m.origin,
			Metadata: localBlock,
		})
	}

	return localIter.Err()
}

func (m replicaMetadataComparer) AddPeerMetadata(peerIter client.PeerBlockMetadataIter) error {
	for peerIter.Next() {
		peer, peerBlock := peerIter.Current()
		blocks := m.metadata.GetOrAdd(peerBlock.ID)
		blocks.GetOrAdd(peerBlock.Start, m.replicaMetadataSlicePool).Add(block.ReplicaMetadata{
			Host:     peer,
			Metadata: peerBlock,
		})
	}

	return peerIter.Err()
}

func (m replicaMetadataComparer) Compare() MetadataComparisonResult {
	var (
		sizeDiff     = NewReplicaSeriesMetadata()
		checkSumDiff = NewReplicaSeriesMetadata()
	)

	for _, entry := range m.metadata.Series().Iter() {
		series := entry.Value()
		for _, b := range series.Metadata.Blocks() {
			bm := b.Metadata()

			var (
				originContainsBlock = false
				sizeVal             int64
				sameSize            = true
				firstSize           = true
				checksumVal         uint32
				sameChecksum        = true
				firstChecksum       = true
			)

			for _, hm := range bm {
				if !originContainsBlock {
					if hm.Host.ID() == m.origin.ID() {
						originContainsBlock = true
					}
				}

				if hm.Metadata.Checksum == nil {
					// Skip metadata that doesn't have a checksum. This usually means that the
					// metadata represents unmerged or pending data. Better to skip for now and
					// repair it once it has been merged as opposed to repairing it now and
					// ping-ponging the same data back and forth between all the repairing nodes.
					//
					// The impact of this is that recently modified data may take longer to be
					// repaired, but it saves a ton of work by preventing nodes from repairing
					// from each other unnecessarily even when they have identical data.
					//
					// TODO(rartoul): Consider skipping series with duplicate metadata as well?
					continue
				}

				// Check size.
				if firstSize {
					sizeVal = hm.Metadata.Size
					firstSize = false
				} else if hm.Metadata.Size != sizeVal {
					sameSize = false
				}

				// If a previous metadata had a checksum and this one does not
				// then assume the checksums mismatch.
				if hm.Metadata.Checksum == nil && !firstChecksum {
					sameChecksum = false
					continue
				}

				// Check checksum.
				if hm.Metadata.Checksum != nil {
					if firstChecksum {
						checksumVal = *hm.Metadata.Checksum
						firstChecksum = false
					} else if *hm.Metadata.Checksum != checksumVal {
						sameChecksum = false
					}
				}
			}

			// If only a subset of hosts in the replica set have sizes, or the sizes differ,
			// we record this block
			if !originContainsBlock || !sameSize {
				sizeDiff.GetOrAdd(series.ID).Add(b)
			}

			// If only a subset of hosts in the replica set have checksums, or the checksums
			// differ, we record this block
			if !originContainsBlock || !sameChecksum {
				checkSumDiff.GetOrAdd(series.ID).Add(b)
			}
		}
	}

	return MetadataComparisonResult{
		NumSeries:           m.metadata.NumSeries(),
		NumBlocks:           m.metadata.NumBlocks(),
		SizeDifferences:     sizeDiff,
		ChecksumDifferences: checkSumDiff,
	}
}

func (m replicaMetadataComparer) Finalize() {
	m.metadata.Close()
}
