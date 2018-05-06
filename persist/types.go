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

package persist

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
)

// Fn is a function that persists a m3db segment for a given ID.
type Fn func(id ident.ID, tags ident.Tags, segment ts.Segment, checksum uint32) error

// Closer is a function that performs cleanup after persisting the data
// blocks for a (shard, blockStart) combination.
type Closer func() error

// PreparedPersist is an object that wraps holds a persist function and a closer.
type PreparedPersist struct {
	Persist Fn
	Close   Closer
}

// Manager manages the internals of persisting data onto storage layer.
type Manager interface {
	// StartPersist begins a flush for a set of shards.
	StartPersist() (Flush, error)
}

// Flush is a persist flush cycle, each shard and block start permutation needs
// to explicility be prepared.
type Flush interface {
	// Prepare prepares writing data for a given (shard, blockStart) combination,
	// returning a PreparedPersist object and any error encountered during
	// preparation if any.
	Prepare(opts PrepareOptions) (PreparedPersist, error)

	// Done marks the flush as complete.
	Done() error
}

// PrepareOptions is the options struct for the PersistManager's Prepare method.
type PrepareOptions struct {
	NamespaceMetadata namespace.Metadata
	BlockStart        time.Time
	SnapshotTime      time.Time
	Shard             uint32
	FileSetType       FileSetType
}

// PrepareSnapshotOptions is the options struct for the Prepare method that contains
// information specific to reading snapshot files
type PrepareSnapshotOptions struct {
	SnapshotTime time.Time
}

// FileSetType is an enum that indicates what type of files a fileset contains
type FileSetType int

func (f FileSetType) String() string {
	switch f {
	case FileSetFlushType:
		return "flush"
	case FileSetSnapshotType:
		return "snapshot"
	}

	return fmt.Sprintf("unknown: %d", f)
}

const (
	// FileSetFlushType indicates that the fileset files contain a complete flush
	FileSetFlushType FileSetType = iota
	// FileSetSnapshotType indicates that the fileset files contain a snapshot
	FileSetSnapshotType
)

// FileSetContentType is an enum that indicates what the contents of files a fileset contains
type FileSetContentType int

func (f FileSetContentType) String() string {
	switch f {
	case FileSetDataContentType:
		return "data"
	case FileSetIndexContentType:
		return "index"
	}
	return fmt.Sprintf("unknown: %d", f)
}

const (
	// FileSetDataContentType indicates that the fileset files contents is time series data
	FileSetDataContentType FileSetContentType = iota
	// FileSetIndexContentType indicates that the fileset files contain time series index metadata
	FileSetIndexContentType
)
