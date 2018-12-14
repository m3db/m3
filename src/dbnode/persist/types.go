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

	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
	"github.com/pborman/uuid"
)

// DataFn is a function that persists a m3db segment for a given ID.
type DataFn func(id ident.ID, tags ident.Tags, segment ts.Segment, checksum uint32) error

// DataCloser is a function that performs cleanup after persisting the data
// blocks for a (shard, blockStart) combination.
type DataCloser func() error

// PreparedDataPersist is an object that wraps holds a persist function and a closer.
type PreparedDataPersist struct {
	Persist DataFn
	Close   DataCloser
}

// CommitlogFile represents a commit log file and its associated metadata.
type CommitlogFile struct {
	FilePath string
	Start    time.Time
	Duration time.Duration
	Index    int64
}

// IndexFn is a function that persists a m3ninx MutableSegment.
type IndexFn func(segment.MutableSegment) error

// IndexCloser is a function that performs cleanup after persisting the index data
// block for a (namespace, blockStart) combination and returns the corresponding
// immutable Segment.
type IndexCloser func() ([]segment.Segment, error)

// PreparedIndexPersist is an object that wraps holds a persist function and a closer.
type PreparedIndexPersist struct {
	Persist IndexFn
	Close   IndexCloser
}

// Manager manages the internals of persisting data onto storage layer.
type Manager interface {
	// StartFlushPersist begins a data flush for a set of shards.
	StartFlushPersist() (FlushPreparer, error)

	// StartSnapshotPersist begins a snapshot for a set of shards.
	StartSnapshotPersist() (SnapshotPreparer, error)

	// StartIndexPersist begins a flush for index data.
	StartIndexPersist() (IndexFlush, error)
}

// Preparer can generated a PreparedDataPersist object for writing data for
// a given (shard, blockstart) combination.
type Preparer interface {
	// Prepare prepares writing data for a given (shard, blockStart) combination,
	// returning a PreparedDataPersist object and any error encountered during
	// preparation if any.
	PrepareData(opts DataPrepareOptions) (PreparedDataPersist, error)
}

// FlushPreparer is a persist flush cycle, each shard and block start permutation needs
// to explicility be prepared.
type FlushPreparer interface {
	Preparer

	// DoneFlush marks the data flush as complete.
	DoneFlush() error
}

// SnapshotPreparer is a persist snapshot cycle, each shard and block start permutation needs
// to explicility be prepared.
type SnapshotPreparer interface {
	Preparer

	// DoneSnapshot marks the snapshot as complete.
	DoneSnapshot(snapshotUUID uuid.UUID, commitLogIdentifier CommitlogFile) error
}

// IndexFlush is a persist flush cycle, each namespace, block combination needs
// to explicility be prepared.
type IndexFlush interface {
	// Prepare prepares writing data for a given ns/blockStart, returning a
	// PreparedIndexPersist object and any error encountered during
	// preparation if any.
	PrepareIndex(opts IndexPrepareOptions) (PreparedIndexPersist, error)

	// DoneIndex marks the index flush as complete.
	DoneIndex() error
}

// DataPrepareOptions is the options struct for the DataFlush's Prepare method.
// nolint: maligned
type DataPrepareOptions struct {
	NamespaceMetadata namespace.Metadata
	BlockStart        time.Time
	Shard             uint32
	FileSetType       FileSetType
	DeleteIfExists    bool
	// Snapshot options are applicable to snapshots (index yes, data yes)
	Snapshot DataPrepareSnapshotOptions
}

// DataPrepareVolumeOptions is the options struct for the prepare method that contains
// information specific to read/writing filesets that have multiple volumes (such as
// snapshots and index file sets).
type DataPrepareVolumeOptions struct {
	VolumeIndex int
}

// IndexPrepareOptions is the options struct for the IndexFlush's Prepare method.
// nolint: maligned
type IndexPrepareOptions struct {
	NamespaceMetadata namespace.Metadata
	BlockStart        time.Time
	FileSetType       FileSetType
	Shards            map[uint32]struct{}
}

// DataPrepareSnapshotOptions is the options struct for the Prepare method that contains
// information specific to read/writing snapshot files.
type DataPrepareSnapshotOptions struct {
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
