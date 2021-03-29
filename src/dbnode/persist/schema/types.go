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

package schema

import (
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
)

// MajorVersion is the major schema version for a set of fileset files,
// this is only incremented when breaking changes are introduced and
// tooling needs to upgrade older files to newer files before a server restart
const MajorVersion = 1

// MinorVersion is the minor schema version for a set of fileset files.
// This is only incremented when *non-breaking* changes are introduced that
// we want to have some level of control around how they're rolled out.
const MinorVersion = 1

// IndexInfo stores metadata information about block filesets.
type IndexInfo struct {
	MajorVersion int64
	BlockStart   int64
	BlockSize    int64
	Entries      int64
	Summaries    IndexSummariesInfo
	BloomFilter  IndexBloomFilterInfo
	SnapshotTime int64
	FileType     persist.FileSetType
	SnapshotID   []byte
	VolumeIndex  int
	MinorVersion int64
}

// IndexSummariesInfo stores metadata about the summaries.
type IndexSummariesInfo struct {
	Summaries int64
}

// IndexBloomFilterInfo stores metadata about the bloom filter.
type IndexBloomFilterInfo struct {
	NumElementsM int64
	NumHashesK   int64
}

// IndexEntry stores entry-level data indexing.
//
// When serialized to disk, the encoder will automatically add the IndexEntryChecksum, a checksum to validate
// the index entry itself, to the end of the entry. That field is not exposed on this struct as this is handled
// transparently by the encoder and decoder. Appending of checksum starts in V3.
type IndexEntry struct {
	Index         int64
	ID            []byte
	Size          int64
	Offset        int64
	DataChecksum  int64
	EncodedTags   []byte
	IndexChecksum int64
}

// WideEntry extends IndexEntry for use with queries, by providing
// an additional metadata checksum field.
type WideEntry struct {
	// WideEntry embeds IndexEntry.
	IndexEntry
	// MetadataChecksum is the computed index metadata checksum.
	// NB: built from ID, DataChecksum, and tags.
	MetadataChecksum int64
}

// WideEntryFilter provides a filter for wide entries.
type WideEntryFilter func(entry WideEntry) (bool, error)

// IndexEntryHasher hashes an index entry.
type IndexEntryHasher interface {
	// HashIndexEntry computes a hash value for this index entry using its ID, tags,
	// and the computed data checksum.
	// NB: not passing the whole IndexEntry because of linter message:
	// "hugeParam: e is heavy (88 bytes); consider passing it by pointer".
	HashIndexEntry(
		id ident.BytesID,
		encodedTags ts.EncodedTags,
		dataChecksum int64,
	) int64
}

// IndexSummary stores a summary of an index entry to lookup.
type IndexSummary struct {
	Index            int64
	ID               []byte
	IndexEntryOffset int64
}

// LogInfo stores summary information about a commit log.
type LogInfo struct {
	// Deprecated fields, left intact as documentation for the actual
	// format on disk.
	DeprecatedDoNotUseStart    int64
	DeprecatedDoNotUseDuration int64

	Index int64
}

// LogEntry stores per-entry data in a commit log
type LogEntry struct {
	Index      uint64
	Create     int64
	Metadata   []byte
	Timestamp  int64
	Value      float64
	Unit       uint32
	Annotation []byte
}

// LogMetadata stores metadata information about a commit log.
type LogMetadata struct {
	ID          []byte
	Namespace   []byte
	Shard       uint32
	EncodedTags []byte
}
