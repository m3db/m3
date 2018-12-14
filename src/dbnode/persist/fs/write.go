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

package fs

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/m3db/bloom"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
	"github.com/pborman/uuid"
)

const (
	// CheckpointFileSizeBytes is the expected size of a valid checkpoint file.
	CheckpointFileSizeBytes = 4
)

var (
	errWriterEncodeTagsDataNotAccessible = errors.New(
		"failed to encode tags: cannot get data")
)

type writer struct {
	blockSize        time.Duration
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode

	summariesPercent                float64
	bloomFilterFalsePositivePercent float64

	infoFdWithDigest           digest.FdWithDigestWriter
	indexFdWithDigest          digest.FdWithDigestWriter
	summariesFdWithDigest      digest.FdWithDigestWriter
	bloomFilterFdWithDigest    digest.FdWithDigestWriter
	dataFdWithDigest           digest.FdWithDigestWriter
	digestFdWithDigestContents digest.FdWithDigestContentsWriter
	checkpointFilePath         string
	indexEntries               indexEntries

	start        time.Time
	snapshotTime time.Time
	snapshotID   uuid.UUID

	currIdx            int64
	currOffset         int64
	encoder            *msgpack.Encoder
	digestBuf          digest.Buffer
	singleCheckedBytes []checked.Bytes
	tagEncoderPool     serialize.TagEncoderPool
	err                error
}

type indexEntry struct {
	index           int64
	id              ident.ID
	tags            ident.Tags
	dataFileOffset  int64
	indexFileOffset int64
	size            uint32
	checksum        uint32
}

type indexEntries []indexEntry

func (e indexEntries) releaseRefs() {
	for i := range e {
		e[i].id = nil
	}
}

func (e indexEntries) Len() int {
	return len(e)
}

func (e indexEntries) Less(i, j int) bool {
	return bytes.Compare(e[i].id.Bytes(), e[j].id.Bytes()) < 0
}

func (e indexEntries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// NewWriter returns a new writer with options.
func NewWriter(opts Options) (DataFileSetWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	bufferSize := opts.WriterBufferSize()
	return &writer{
		filePathPrefix:                  opts.FilePathPrefix(),
		newFileMode:                     opts.NewFileMode(),
		newDirectoryMode:                opts.NewDirectoryMode(),
		summariesPercent:                opts.IndexSummariesPercent(),
		bloomFilterFalsePositivePercent: opts.IndexBloomFilterFalsePositivePercent(),
		infoFdWithDigest:                digest.NewFdWithDigestWriter(bufferSize),
		indexFdWithDigest:               digest.NewFdWithDigestWriter(bufferSize),
		summariesFdWithDigest:           digest.NewFdWithDigestWriter(bufferSize),
		bloomFilterFdWithDigest:         digest.NewFdWithDigestWriter(bufferSize),
		dataFdWithDigest:                digest.NewFdWithDigestWriter(bufferSize),
		digestFdWithDigestContents:      digest.NewFdWithDigestContentsWriter(bufferSize),
		encoder:                         msgpack.NewEncoder(),
		digestBuf:                       digest.NewBuffer(),
		singleCheckedBytes:              make([]checked.Bytes, 1),
		tagEncoderPool:                  opts.TagEncoderPool(),
	}, nil
}

// Open initializes the internal state for writing to the given shard,
// specifically creating the shard directory if it doesn't exist, and
// opening / truncating files associated with that shard for writing.
func (w *writer) Open(opts DataWriterOpenOptions) error {
	var (
		nextSnapshotIndex int
		err               error
		namespace         = opts.Identifier.Namespace
		shard             = opts.Identifier.Shard
		blockStart        = opts.Identifier.BlockStart
	)

	w.blockSize = opts.BlockSize
	w.start = blockStart
	w.snapshotTime = opts.Snapshot.SnapshotTime
	w.snapshotID = opts.Snapshot.SnapshotID
	w.currIdx = 0
	w.currOffset = 0
	w.err = nil

	var (
		shardDir            string
		infoFilepath        string
		indexFilepath       string
		summariesFilepath   string
		bloomFilterFilepath string
		dataFilepath        string
		digestFilepath      string
	)
	switch opts.FileSetType {
	case persist.FileSetSnapshotType:
		shardDir = ShardSnapshotsDirPath(w.filePathPrefix, namespace, shard)
		// Can't do this outside of the switch statement because we need to make sure
		// the directory exists before calling NextSnapshotFileSetIndex
		if err := os.MkdirAll(shardDir, w.newDirectoryMode); err != nil {
			return err
		}

		nextSnapshotIndex = opts.Identifier.VolumeIndex
		w.checkpointFilePath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, checkpointFileSuffix)
		infoFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, infoFileSuffix)
		indexFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, indexFileSuffix)
		summariesFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, summariesFileSuffix)
		bloomFilterFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, bloomFilterFileSuffix)
		dataFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, dataFileSuffix)
		digestFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, nextSnapshotIndex, digestFileSuffix)
	case persist.FileSetFlushType:
		shardDir = ShardDataDirPath(w.filePathPrefix, namespace, shard)
		if err := os.MkdirAll(shardDir, w.newDirectoryMode); err != nil {
			return err
		}

		w.checkpointFilePath = filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
		infoFilepath = filesetPathFromTime(shardDir, blockStart, infoFileSuffix)
		indexFilepath = filesetPathFromTime(shardDir, blockStart, indexFileSuffix)
		summariesFilepath = filesetPathFromTime(shardDir, blockStart, summariesFileSuffix)
		bloomFilterFilepath = filesetPathFromTime(shardDir, blockStart, bloomFilterFileSuffix)
		dataFilepath = filesetPathFromTime(shardDir, blockStart, dataFileSuffix)
		digestFilepath = filesetPathFromTime(shardDir, blockStart, digestFileSuffix)
	default:
		return fmt.Errorf("unable to open reader with fileset type: %s", opts.FileSetType)
	}

	var infoFd, indexFd, summariesFd, bloomFilterFd, dataFd, digestFd *os.File
	err = openFiles(w.openWritable,
		map[string]**os.File{
			infoFilepath:        &infoFd,
			indexFilepath:       &indexFd,
			summariesFilepath:   &summariesFd,
			bloomFilterFilepath: &bloomFilterFd,
			dataFilepath:        &dataFd,
			digestFilepath:      &digestFd,
		},
	)
	if err != nil {
		return err
	}

	w.infoFdWithDigest.Reset(infoFd)
	w.indexFdWithDigest.Reset(indexFd)
	w.summariesFdWithDigest.Reset(summariesFd)
	w.bloomFilterFdWithDigest.Reset(bloomFilterFd)
	w.dataFdWithDigest.Reset(dataFd)
	w.digestFdWithDigestContents.Reset(digestFd)

	return nil
}

func (w *writer) writeData(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	written, err := w.dataFdWithDigest.Write(data)
	if err != nil {
		return err
	}
	w.currOffset += int64(written)
	return nil
}

func (w *writer) Write(
	id ident.ID,
	tags ident.Tags,
	data checked.Bytes,
	checksum uint32,
) error {
	w.singleCheckedBytes[0] = data
	return w.WriteAll(id, tags, w.singleCheckedBytes, checksum)
}

func (w *writer) WriteAll(
	id ident.ID,
	tags ident.Tags,
	data []checked.Bytes,
	checksum uint32,
) error {
	if w.err != nil {
		return w.err
	}

	if err := w.writeAll(id, tags, data, checksum); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeAll(
	id ident.ID,
	tags ident.Tags,
	data []checked.Bytes,
	checksum uint32,
) error {
	var size int64
	for _, d := range data {
		if d == nil {
			continue
		}
		size += int64(d.Len())
	}
	if size == 0 {
		return nil
	}

	entry := indexEntry{
		index:          w.currIdx,
		id:             id,
		tags:           tags,
		dataFileOffset: w.currOffset,
		size:           uint32(size),
		checksum:       checksum,
	}
	for _, d := range data {
		if d == nil {
			continue
		}
		if err := w.writeData(d.Bytes()); err != nil {
			return err
		}
	}

	w.indexEntries = append(w.indexEntries, entry)
	w.currIdx++

	return nil
}

func (w *writer) Close() error {
	err := w.close()
	if w.err != nil {
		return w.err
	}
	if err != nil {
		w.err = err
		return err
	}
	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := w.writeCheckpointFile(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) close() error {
	if err := w.writeIndexRelatedFiles(); err != nil {
		return err
	}

	if err := w.digestFdWithDigestContents.WriteDigests(
		w.infoFdWithDigest.Digest().Sum32(),
		w.indexFdWithDigest.Digest().Sum32(),
		w.summariesFdWithDigest.Digest().Sum32(),
		w.bloomFilterFdWithDigest.Digest().Sum32(),
		w.dataFdWithDigest.Digest().Sum32(),
	); err != nil {
		return err
	}

	return closeAll(
		w.infoFdWithDigest,
		w.indexFdWithDigest,
		w.summariesFdWithDigest,
		w.bloomFilterFdWithDigest,
		w.dataFdWithDigest,
		w.digestFdWithDigestContents,
	)
}

func (w *writer) writeCheckpointFile() error {
	fd, err := w.openWritable(w.checkpointFilePath)
	if err != nil {
		return err
	}
	digestChecksum := w.digestFdWithDigestContents.Digest().Sum32()
	if err := w.digestBuf.WriteDigestToFile(fd, digestChecksum); err != nil {
		// NB(prateek): intentionally skipping fd.Close() error, as failure
		// to write takes precedence over failure to close the file
		fd.Close()
		return err
	}
	return fd.Close()
}

func (w *writer) openWritable(filePath string) (*os.File, error) {
	return OpenWritable(filePath, w.newFileMode)
}

func (w *writer) writeIndexRelatedFiles() error {
	summariesApprox := float64(len(w.indexEntries)) * w.summariesPercent
	summaryEvery := 0
	if summariesApprox > 0 {
		summaryEvery = int(math.Floor(float64(len(w.indexEntries)) / summariesApprox))
	}

	// Write the index entries and calculate the bloom filter
	n, p := uint(w.currIdx), w.bloomFilterFalsePositivePercent
	m, k := bloom.EstimateFalsePositiveRate(n, p)
	bloomFilter := bloom.NewBloomFilter(m, k)

	err := w.writeIndexFileContents(bloomFilter, summaryEvery)
	if err != nil {
		return err
	}

	// Write summaries and start zeroing out memory to avoid holding onto refs
	summaries, err := w.writeSummariesFileContents(summaryEvery)
	if err != nil {
		return err
	}

	// Reset summaries slice to avoid allocs for next shard flush, this avoids
	// leaking memory. Be sure to release all refs before resizing to avoid GC
	// holding roots.
	w.indexEntries.releaseRefs()
	w.indexEntries = w.indexEntries[:0]

	// Write the bloom filter bitset out
	if err := w.writeBloomFilterFileContents(bloomFilter); err != nil {
		return err
	}

	return w.writeInfoFileContents(bloomFilter, summaries)
}

func (w *writer) writeIndexFileContents(
	bloomFilter *bloom.BloomFilter,
	summaryEvery int,
) error {
	// NB(r): Write the index file in order, in the future we could write
	// these in order to avoid this sort at the end however that does require
	// significant changes in the storage/databaseShard to store things in order
	// which would sacrifice O(1) insertion of new series we currently have.
	//
	// Probably do want to do this at the end still however so we don't stripe
	// writes to two different files during the write loop.
	sort.Sort(w.indexEntries)

	var (
		offset      int64
		prevID      []byte
		tagsIter    = ident.NewTagsIterator(ident.Tags{})
		tagsEncoder = w.tagEncoderPool.Get()
	)
	defer tagsEncoder.Finalize()
	for i := range w.indexEntries {
		id := w.indexEntries[i].id.Bytes()
		// Need to check if i > 0 or we can never write an empty string ID
		if i > 0 && bytes.Equal(id, prevID) {
			// Should never happen, Write() should only be called once per ID
			return fmt.Errorf("encountered duplicate ID: %s", id)
		}

		var encodedTags []byte
		if tags := w.indexEntries[i].tags; tags.Values() != nil {
			tagsIter.Reset(tags)
			tagsEncoder.Reset()
			if err := tagsEncoder.Encode(tagsIter); err != nil {
				return err
			}
			data, ok := tagsEncoder.Data()
			if !ok {
				return errWriterEncodeTagsDataNotAccessible
			}
			encodedTags = data.Bytes()
		}

		entry := schema.IndexEntry{
			Index:       w.indexEntries[i].index,
			ID:          id,
			Size:        int64(w.indexEntries[i].size),
			Offset:      w.indexEntries[i].dataFileOffset,
			Checksum:    int64(w.indexEntries[i].checksum),
			EncodedTags: encodedTags,
		}

		w.encoder.Reset()
		if err := w.encoder.EncodeIndexEntry(entry); err != nil {
			return err
		}

		data := w.encoder.Bytes()
		if _, err := w.indexFdWithDigest.Write(data); err != nil {
			return err
		}

		// Add to the bloom filter, note this must be zero alloc or else this will
		// cause heavy GC churn as we flush millions of series at end of each
		// time window
		bloomFilter.Add(id)

		if i%summaryEvery == 0 {
			// Capture the offset for when we write this summary back, only capture
			// for every summary we'll actually write to avoid a few memcopies
			w.indexEntries[i].indexFileOffset = offset
		}

		offset += int64(len(data))

		prevID = id
	}

	return nil
}

func (w *writer) writeSummariesFileContents(
	summaryEvery int,
) (int, error) {
	summaries := 0
	for i := range w.indexEntries {
		if i%summaryEvery != 0 {
			continue
		}

		summary := schema.IndexSummary{
			Index:            w.indexEntries[i].index,
			ID:               w.indexEntries[i].id.Bytes(),
			IndexEntryOffset: w.indexEntries[i].indexFileOffset,
		}

		w.encoder.Reset()
		if err := w.encoder.EncodeIndexSummary(summary); err != nil {
			return 0, err
		}

		data := w.encoder.Bytes()
		if _, err := w.summariesFdWithDigest.Write(data); err != nil {
			return 0, err
		}

		summaries++
	}

	return summaries, nil
}

func (w *writer) writeBloomFilterFileContents(
	bloomFilter *bloom.BloomFilter,
) error {
	return bloomFilter.BitSet().Write(w.bloomFilterFdWithDigest)
}

func (w *writer) writeInfoFileContents(
	bloomFilter *bloom.BloomFilter,
	summaries int,
) error {
	snapshotBytes, err := w.snapshotID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("error marshaling snapshot ID into bytes: %v", err)
	}

	info := schema.IndexInfo{
		BlockStart:   xtime.ToNanoseconds(w.start),
		SnapshotTime: xtime.ToNanoseconds(w.snapshotTime),
<<<<<<< HEAD
		SnapshotID:   w.snapshotID,
=======
		SnapshotID:   snapshotBytes,
>>>>>>> squash
		BlockSize:    int64(w.blockSize),
		Entries:      w.currIdx,
		MajorVersion: schema.MajorVersion,
		Summaries: schema.IndexSummariesInfo{
			Summaries: int64(summaries),
		},
		BloomFilter: schema.IndexBloomFilterInfo{
			NumElementsM: int64(bloomFilter.M()),
			NumHashesK:   int64(bloomFilter.K()),
		},
	}

	w.encoder.Reset()
	if err := w.encoder.EncodeIndexInfo(info); err != nil {
		return err
	}

	_, err = w.infoFdWithDigest.Write(w.encoder.Bytes())
	return err
}
