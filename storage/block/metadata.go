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
	"time"

	"github.com/m3db/m3db/ts"
)

type metadata struct {
	start    time.Time
	size     int64
	checksum *uint32
}

// NewMetadata creates a new block metadata
func NewMetadata(start time.Time, size int64, checksum *uint32) Metadata {
	return metadata{
		start:    start,
		size:     size,
		checksum: checksum,
	}
}

func (m metadata) Start() time.Time  { return m.start }
func (m metadata) Size() int64       { return m.size }
func (m metadata) Checksum() *uint32 { return m.checksum }

type blocksMetadata struct {
	id     ts.ID
	blocks []Metadata
}

// NewBlocksMetadata creates a new blocks metadata
func NewBlocksMetadata(id ts.ID, blocks []Metadata) BlocksMetadata {
	return blocksMetadata{id: id, blocks: blocks}
}

func (m blocksMetadata) ID() ts.ID          { return m.id }
func (m blocksMetadata) Blocks() []Metadata { return m.blocks }
