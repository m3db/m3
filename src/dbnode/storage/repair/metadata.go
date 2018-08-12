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
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

const (
	defaultReplicaBlocksMetadataCapacity = 1
	defaultReplicaSeriesMetadataCapacity = 4096
)

type hostBlockMetadataSlice struct {
	metadata []HostBlockMetadata
	pool     HostBlockMetadataSlicePool
}

func newHostBlockMetadataSlice() HostBlockMetadataSlice {
	return &hostBlockMetadataSlice{}
}

func newPooledHostBlockMetadataSlice(metadata []HostBlockMetadata, pool HostBlockMetadataSlicePool) HostBlockMetadataSlice {
	return &hostBlockMetadataSlice{metadata: metadata, pool: pool}
}

func (s *hostBlockMetadataSlice) Add(metadata HostBlockMetadata) {
	s.metadata = append(s.metadata, metadata)
}

func (s *hostBlockMetadataSlice) Metadata() []HostBlockMetadata {
	return s.metadata
}

func (s *hostBlockMetadataSlice) Reset() {
	var zeroed HostBlockMetadata
	for i := range s.metadata {
		s.metadata[i] = zeroed
	}
	s.metadata = s.metadata[:0]
}

func (s *hostBlockMetadataSlice) Close() {
	if s.pool != nil {
		s.pool.Put(s)
	}
}

type replicaBlockMetadata struct {
	start    time.Time
	metadata HostBlockMetadataSlice
}

// NewReplicaBlockMetadata creates a new replica block metadata
func NewReplicaBlockMetadata(start time.Time, p HostBlockMetadataSlice) ReplicaBlockMetadata {
	return replicaBlockMetadata{start: start, metadata: p}
}

func (m replicaBlockMetadata) Start() time.Time               { return m.start }
func (m replicaBlockMetadata) Metadata() []HostBlockMetadata  { return m.metadata.Metadata() }
func (m replicaBlockMetadata) Add(metadata HostBlockMetadata) { m.metadata.Add(metadata) }
func (m replicaBlockMetadata) Close()                         { m.metadata.Close() }

type replicaBlocksMetadata map[xtime.UnixNano]ReplicaBlockMetadata

// NewReplicaBlocksMetadata creates a new replica blocks metadata
func NewReplicaBlocksMetadata() ReplicaBlocksMetadata {
	return make(replicaBlocksMetadata, defaultReplicaBlocksMetadataCapacity)
}

func (m replicaBlocksMetadata) NumBlocks() int64                                { return int64(len(m)) }
func (m replicaBlocksMetadata) Blocks() map[xtime.UnixNano]ReplicaBlockMetadata { return m }
func (m replicaBlocksMetadata) Add(block ReplicaBlockMetadata) {
	m[xtime.ToUnixNano(block.Start())] = block
}

func (m replicaBlocksMetadata) GetOrAdd(start time.Time, p HostBlockMetadataSlicePool) ReplicaBlockMetadata {
	startNano := xtime.ToUnixNano(start)
	block, exists := m[startNano]
	if exists {
		return block
	}
	block = NewReplicaBlockMetadata(start, p.Get())
	m[startNano] = block
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
	replicas                   int
	metadata                   ReplicaSeriesMetadata
	hostBlockMetadataSlicePool HostBlockMetadataSlicePool
}

// NewReplicaMetadataComparer creates a new replica metadata comparer
func NewReplicaMetadataComparer(replicas int, opts Options) ReplicaMetadataComparer {
	return replicaMetadataComparer{
		replicas:                   replicas,
		metadata:                   NewReplicaSeriesMetadata(),
		hostBlockMetadataSlicePool: opts.HostBlockMetadataSlicePool(),
	}
}

func (m replicaMetadataComparer) AddLocalMetadata(origin topology.Host, localIter block.FilteredBlocksMetadataIter) error {
	for localIter.Next() {
		id, block := localIter.Current()
		blocks := m.metadata.GetOrAdd(id)
		blocks.GetOrAdd(block.Start, m.hostBlockMetadataSlicePool).Add(HostBlockMetadata{
			Host:     origin,
			Size:     block.Size,
			Checksum: block.Checksum,
		})
	}

	return localIter.Err()
}

func (m replicaMetadataComparer) AddPeerMetadata(peerIter client.PeerBlockMetadataIter) error {
	for peerIter.Next() {
		peer, peerBlock := peerIter.Current()
		blocks := m.metadata.GetOrAdd(peerBlock.ID)
		blocks.GetOrAdd(peerBlock.Start, m.hostBlockMetadataSlicePool).Add(HostBlockMetadata{
			Host:     peer,
			Size:     peerBlock.Size,
			Checksum: peerBlock.Checksum,
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
				sizeDiff.GetOrAdd(series.ID).Add(b)
			}

			// If only a subset of hosts in the replica set have checksums, or the checksums
			// differ, we record this block
			if !(numHostsWithChecksum == m.replicas && sameChecksum) {
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
