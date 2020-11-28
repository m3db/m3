// Copyright (c) 2020 Uber Technologies, Inc.
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

package xio

import (
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
)

// WideEntry is an entry from the index file which can be passed to
// SeekUsingIndexEntry to seek to the data for that entry.
type WideEntry struct {
	finalized bool

	Shard            uint32
	ID               ident.ID
	Size             int64
	Offset           int64
	DataChecksum     int64
	EncodedTags      checked.Bytes
	MetadataChecksum int64
	Data             checked.Bytes
}

// Empty returns whether the wide entry is empty and not found.
func (c *WideEntry) Empty() bool {
	return *c == WideEntry{}
}

// Finalize finalizes the wide entry.
func (c *WideEntry) Finalize() {
	if c.Empty() || c.finalized {
		return
	}

	c.finalized = true
	if c.EncodedTags != nil {
		c.EncodedTags.DecRef()
		c.EncodedTags.Finalize()
		c.EncodedTags = nil
	}

	if c.ID != nil {
		c.ID.Finalize()
		c.ID = nil
	}

	if c.Data != nil {
		c.Data.DecRef()
		c.Data.Finalize()
		c.Data = nil
	}
}
