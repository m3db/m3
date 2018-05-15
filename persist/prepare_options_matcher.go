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

package persist

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/storage/namespace"
)

// DataPrepareOptionsMatcher satisfies the gomock.Matcher interface for DataPrepareOptions
// nolint: maligned
type DataPrepareOptionsMatcher struct {
	NsMetadata     namespace.Metadata
	Shard          uint32
	BlockStart     time.Time
	SnapshotTime   time.Time
	FileSetType    FileSetType
	DeleteIfExists bool
}

// Matches determines whether a DataPrepareOptionsMatcher matches a DataPrepareOptions
func (p DataPrepareOptionsMatcher) Matches(x interface{}) bool {
	prepareOptions, ok := x.(DataPrepareOptions)
	if !ok {
		return false
	}

	if !p.NsMetadata.Equal(prepareOptions.NamespaceMetadata) {
		return false
	}
	if p.Shard != prepareOptions.Shard {
		return false
	}
	if !p.SnapshotTime.Equal(prepareOptions.SnapshotTime) {
		return false
	}
	if !p.BlockStart.Equal(prepareOptions.BlockStart) {
		return false
	}
	if p.FileSetType != prepareOptions.FileSetType {
		return false
	}
	if p.DeleteIfExists != prepareOptions.DeleteIfExists {
		return false
	}

	return true
}

func (p DataPrepareOptionsMatcher) String() string {
	return fmt.Sprintf(
		"NSMetadata: %s, Shard: %d, BlockStart: %d, SnapshotTime: %d, FileSetType: %s, DeleteIfExists: %t",
		p.NsMetadata.ID().String(), p.Shard, p.BlockStart.Unix(), p.SnapshotTime.Unix(), p.FileSetType, p.DeleteIfExists)
}
