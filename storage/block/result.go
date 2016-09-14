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

package block

import (
	"sort"
	"time"

	"github.com/m3db/m3db/x/io"
)

type fetchBlockResult struct {
	start   time.Time
	readers []xio.SegmentReader
	err     error
}

// NewFetchBlockResult creates a new fetch block result
func NewFetchBlockResult(start time.Time, readers []xio.SegmentReader, err error) FetchBlockResult {
	return fetchBlockResult{start: start, readers: readers, err: err}
}

func (b fetchBlockResult) Start() time.Time             { return b.start }
func (b fetchBlockResult) Readers() []xio.SegmentReader { return b.readers }
func (b fetchBlockResult) Err() error                   { return b.err }

type fetchBlockResultByTimeAscending []FetchBlockResult

func (e fetchBlockResultByTimeAscending) Len() int           { return len(e) }
func (e fetchBlockResultByTimeAscending) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e fetchBlockResultByTimeAscending) Less(i, j int) bool { return e[i].Start().Before(e[j].Start()) }

// SortFetchBlockResultByTimeAscending sorts fetch block results in time ascending order
func SortFetchBlockResultByTimeAscending(results []FetchBlockResult) {
	sort.Sort(fetchBlockResultByTimeAscending(results))
}

type fetchBlockMetadataResult struct {
	start    time.Time
	size     *int64
	checksum *uint32
	err      error
}

// NewFetchBlockMetadataResult creates a new fetch block metadata result.
func NewFetchBlockMetadataResult(
	start time.Time,
	size *int64,
	checksum *uint32,
	err error,
) FetchBlockMetadataResult {
	return fetchBlockMetadataResult{
		start:    start,
		size:     size,
		checksum: checksum,
		err:      err,
	}
}

func (r fetchBlockMetadataResult) Start() time.Time  { return r.start }
func (r fetchBlockMetadataResult) Size() *int64      { return r.size }
func (r fetchBlockMetadataResult) Checksum() *uint32 { return r.checksum }
func (r fetchBlockMetadataResult) Err() error        { return r.err }

type fetchBlockMetadataResultByTimeAscending []FetchBlockMetadataResult

func (a fetchBlockMetadataResultByTimeAscending) Len() int      { return len(a) }
func (a fetchBlockMetadataResultByTimeAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a fetchBlockMetadataResultByTimeAscending) Less(i, j int) bool {
	return a[i].Start().Before(a[j].Start())
}

// SortFetchBlockMetadataResultByTimeAscending sorts fetch block metadata result array in time ascending order
func SortFetchBlockMetadataResultByTimeAscending(metadata []FetchBlockMetadataResult) {
	sort.Sort(fetchBlockMetadataResultByTimeAscending(metadata))
}

type fetchBlocksMetadataResult struct {
	id     string
	blocks []FetchBlockMetadataResult
}

// NewFetchBlocksMetadataResult creates new database blocks metadata
func NewFetchBlocksMetadataResult(id string, blocks []FetchBlockMetadataResult) FetchBlocksMetadataResult {
	return fetchBlocksMetadataResult{id: id, blocks: blocks}
}

func (m fetchBlocksMetadataResult) ID() string                         { return m.id }
func (m fetchBlocksMetadataResult) Blocks() []FetchBlockMetadataResult { return m.blocks }

type filteredBlocksMetadataIter struct {
	start     time.Time
	end       time.Time
	blockSize time.Duration
	res       []FetchBlocksMetadataResult
	id        string
	metadata  Metadata
	resIdx    int
	blockIdx  int
}

// NewFilteredBlocksMetadataIter creates a new filtered blocks metadata iterator
func NewFilteredBlocksMetadataIter(
	start, end time.Time,
	blockSize time.Duration,
	res []FetchBlocksMetadataResult,
) FilteredBlocksMetadataIter {
	return &filteredBlocksMetadataIter{
		start:     start,
		end:       end,
		blockSize: blockSize,
		res:       res,
	}
}

func (it *filteredBlocksMetadataIter) Next() bool {
	if it.resIdx >= len(it.res) {
		return false
	}
	blocks := it.res[it.resIdx].Blocks()
	for it.blockIdx < len(blocks) {
		block := blocks[it.blockIdx]
		if block.Err() != nil {
			it.blockIdx++
			continue
		}
		var (
			blockStart = block.Start()
			blockEnd   = blockStart.Add(it.blockSize)
		)
		if !it.start.Before(blockEnd) || !blockStart.Before(it.end) {
			it.blockIdx++
			continue
		}
		break
	}
	if it.blockIdx >= len(blocks) {
		it.resIdx++
		it.blockIdx = 0
		return it.Next()
	}
	it.id = it.res[it.resIdx].ID()
	block := blocks[it.blockIdx]
	size := int64(0)
	if block.Size() != nil {
		size = *block.Size()
	}
	it.metadata = NewMetadata(block.Start(), size, block.Checksum())
	it.blockIdx++
	return true
}

func (it *filteredBlocksMetadataIter) Current() (string, Metadata) {
	return it.id, it.metadata
}
