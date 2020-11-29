// Copyright (c) 2018 Uber Technologies, Inc.
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

package fs

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist"
)

// ReaderOpenOptionsMatcher is a matcher for the DataReaderOpenOptions struct
type ReaderOpenOptionsMatcher struct {
	ID               FileSetFileIdentifier
	FileSetType      persist.FileSetType
	StreamingEnabled bool
}

// Matches determine whether m matches a DataWriterOpenOptions
func (m ReaderOpenOptionsMatcher) Matches(x interface{}) bool {
	readerOpenOptions, ok := x.(DataReaderOpenOptions)
	if !ok {
		return false
	}

	if !m.ID.Namespace.Equal(readerOpenOptions.Identifier.Namespace) {
		return false
	}
	if m.ID.Shard != readerOpenOptions.Identifier.Shard {
		return false
	}
	if !m.ID.BlockStart.Equal(readerOpenOptions.Identifier.BlockStart) {
		return false
	}
	if m.ID.VolumeIndex != readerOpenOptions.Identifier.VolumeIndex {
		return false
	}
	if m.FileSetType != readerOpenOptions.FileSetType {
		return false
	}
	if m.StreamingEnabled != readerOpenOptions.StreamingEnabled {
		return false
	}

	return true
}

func (m ReaderOpenOptionsMatcher) String() string {
	return fmt.Sprintf(
		"namespace: %s, shard: %d, blockstart: %d, volumeIndex: %d, filesetType: %s, streamingEnabled: %t", // nolint: lll
		m.ID.Namespace.String(), m.ID.Shard, m.ID.BlockStart.Unix(), m.ID.VolumeIndex,
		m.FileSetType, m.StreamingEnabled,
	)
}
