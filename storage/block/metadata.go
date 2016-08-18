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
)

type databaseBlockMetadata struct {
	start time.Time
	size  *int64
}

// NewDatabaseBlockMetadata creates a new database block metadata
func NewDatabaseBlockMetadata(start time.Time, size *int64) DatabaseBlockMetadata {
	return databaseBlockMetadata{start: start, size: size}
}

func (m databaseBlockMetadata) Start() time.Time { return m.start }
func (m databaseBlockMetadata) Size() *int64     { return m.size }

type metadataByTimeAscending []DatabaseBlockMetadata

func (a metadataByTimeAscending) Len() int           { return len(a) }
func (a metadataByTimeAscending) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a metadataByTimeAscending) Less(i, j int) bool { return a[i].Start().Before(a[j].Start()) }

// SortMetadataByTimeAscending sorts block metadata array in time ascending order
func SortMetadataByTimeAscending(metadata []DatabaseBlockMetadata) {
	sort.Sort(metadataByTimeAscending(metadata))
}

type databaseBlocksMetadata struct {
	id     string
	blocks []DatabaseBlockMetadata
}

// NewDatabaseBlocksMetadata creates new database blocks metadata
func NewDatabaseBlocksMetadata(id string, blocks []DatabaseBlockMetadata) DatabaseBlocksMetadata {
	return databaseBlocksMetadata{id: id, blocks: blocks}
}

func (m databaseBlocksMetadata) ID() string                      { return m.id }
func (m databaseBlocksMetadata) Blocks() []DatabaseBlockMetadata { return m.blocks }
