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
	"time"

	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3x/ident"
)

// WriterOpenOptionsMatcher satisfies the gomock.Matcher interface for WriterOpenOptions
type WriterOpenOptionsMatcher struct {
	Namespace    ident.ID
	BlockSize    time.Duration
	Shard        uint32
	BlockStart   time.Time
	SnapshotTime time.Time
	FilesetType  persist.FilesetType
}

// Matches determine whether m matches a WriterOpenOptions
func (m WriterOpenOptionsMatcher) Matches(x interface{}) bool {
	writerOpenOptions, ok := x.(WriterOpenOptions)
	if !ok {
		return false
	}

	if !m.Namespace.Equal(writerOpenOptions.Namespace) {
		return false
	}
	if m.BlockSize != writerOpenOptions.BlockSize {
		return false
	}
	if m.Shard != writerOpenOptions.Shard {
		return false
	}
	if !m.BlockStart.Equal(writerOpenOptions.BlockStart) {
		return false
	}
	if !m.SnapshotTime.Equal(writerOpenOptions.SnapshotTime) {
		return false
	}
	if m.FilesetType != writerOpenOptions.FilesetType {
		return false
	}

	return true
}

func (m WriterOpenOptionsMatcher) String() string {
	return fmt.Sprintf(
		"namespace: %s, blocksize: %d, shard: %d, blockstart: %d, snapshotTime: %d, filesetType: %s",
		m.Namespace.String(), m.BlockSize, m.Shard, m.BlockStart.Unix(), m.SnapshotTime.Unix(), m.FilesetType,
	)
}
