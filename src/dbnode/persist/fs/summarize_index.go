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
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/cespare/xxhash"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/mmap"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

type summarizer struct {
	opts          Options
	hugePagesOpts mmap.HugeTLBOptions

	filePathPrefix string
	namespace      ident.ID

	start     time.Time
	blockSize time.Duration

	infoFdWithDigest           digest.FdWithDigestReader
	digestFdWithDigestContents digest.FdWithDigestContentsReader

	indexFd                 *os.File
	indexMmap               mmap.Descriptor
	indexDecoderStream      dataFileSetReaderDecoderStream
	indexEntriesByOffsetAsc []schema.IndexEntry

	entries      int
	metadataRead int
	decoder      *msgpack.Decoder
	digestBuf    digest.Buffer

	expectedInfoDigest     uint32
	expectedIndexDigest    uint32
	expectedDigestOfDigest uint32
	shard                  uint32
	volume                 int
	open                   bool

	orderedByIndex bool
}

// NewSummarizer returns a new summarizer and expects all files to exist. Will
// read the index info in full on call to Open.
func NewSummarizer(
	opts Options,
) (IndexFileSetSummarizer, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &summarizer{
		// When initializing new fields that should be static, be sure to save
		// and reset them after Close() resets the fields to all default values.
		opts:           opts,
		filePathPrefix: opts.FilePathPrefix(),
		hugePagesOpts: mmap.HugeTLBOptions{
			Enabled:   opts.MmapEnableHugeTLB(),
			Threshold: opts.MmapHugeTLBThreshold(),
		},
		infoFdWithDigest:           digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(opts.InfoReaderBufferSize()),
		indexDecoderStream:         newReaderDecoderStream(),
		decoder:                    msgpack.NewDecoder(opts.DecodingOptions()),
		digestBuf:                  digest.NewBuffer(),
	}, nil
}

func (r *summarizer) Open(opts DataReaderOpenOptions) error {
	var (
		namespace   = opts.Identifier.Namespace
		shard       = opts.Identifier.Shard
		blockStart  = opts.Identifier.BlockStart
		volumeIndex = opts.Identifier.VolumeIndex
		err         error
	)

	var (
		shardDir           string
		checkpointFilepath string
		infoFilepath       string
		digestFilepath     string
		indexFilepath      string
	)

	r.orderedByIndex = opts.OrderedByIndex

	switch opts.FileSetType {
	case persist.FileSetSnapshotType:
		shardDir = ShardSnapshotsDirPath(r.filePathPrefix, namespace, shard)
		checkpointFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, checkpointFileSuffix)
		infoFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, infoFileSuffix)
		digestFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, digestFileSuffix)
		indexFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, indexFileSuffix)
	case persist.FileSetFlushType:
		shardDir = ShardDataDirPath(r.filePathPrefix, namespace, shard)

		isLegacy := false
		if volumeIndex == 0 {
			isLegacy, err = isFirstVolumeLegacy(shardDir, blockStart, checkpointFileSuffix)
			if err != nil {
				return err
			}
		}

		checkpointFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, checkpointFileSuffix, isLegacy)
		infoFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, infoFileSuffix, isLegacy)
		digestFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, digestFileSuffix, isLegacy)
		indexFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, indexFileSuffix, isLegacy)
	default:
		return fmt.Errorf("unable to open summarizer with fileset type: %s", opts.FileSetType)
	}

	// If there is no checkpoint file, don't read the data files.
	digest, err := readCheckpointFile(checkpointFilepath, r.digestBuf)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest

	var infoFd, digestFd *os.File
	err = openFiles(os.Open, map[string]**os.File{
		infoFilepath:   &infoFd,
		digestFilepath: &digestFd,
	})
	if err != nil {
		return err
	}

	r.infoFdWithDigest.Reset(infoFd)
	r.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		r.infoFdWithDigest.Close()
		r.digestFdWithDigestContents.Close()
	}()

	result, err := mmap.Files(os.Open, map[string]mmap.FileDesc{
		indexFilepath: mmap.FileDesc{
			File:       &r.indexFd,
			Descriptor: &r.indexMmap,
			Options: mmap.Options{
				Read:    true,
				HugeTLB: r.hugePagesOpts,
				ReporterOptions: mmap.ReporterOptions{
					Context: mmap.Context{
						Name: mmapPersistFsDataIndexName,
					},
					Reporter: r.opts.MmapReporter(),
				},
			},
		},
	})
	if err != nil {
		return err
	}

	if warning := result.Warning; warning != nil {
		logger := r.opts.InstrumentOptions().Logger()
		logger.Warn("warning while mmapping files in summarizer", zap.Error(warning))
	}

	r.indexDecoderStream.Reset(r.indexMmap.Bytes)
	if err := r.readDigest(); err != nil {
		// Try to close if failed to read
		r.Close()
		return err
	}
	infoStat, err := infoFd.Stat()
	if err != nil {
		r.Close()
		return err
	}
	if err := r.readInfo(int(infoStat.Size())); err != nil {
		r.Close()
		return err
	}

	r.decoder.Reset(r.indexDecoderStream)
	r.open = true
	r.namespace = namespace
	r.shard = shard

	return nil
}

func (r *summarizer) Status() DataFileSetReaderStatus {
	return DataFileSetReaderStatus{
		Open:       r.open,
		Namespace:  r.namespace,
		Shard:      r.shard,
		Volume:     r.volume,
		BlockStart: r.start,
		BlockSize:  r.blockSize,
	}
}

func (r *summarizer) readDigest() error {
	fsDigests, err := readFileSetDigests(r.digestFdWithDigestContents)
	if err != nil {
		return err
	}

	err = r.digestFdWithDigestContents.Validate(r.expectedDigestOfDigest)
	if err != nil {
		return err
	}

	// Note that we skip over the summaries file digest here which is available,
	// but we don't need
	r.expectedInfoDigest = fsDigests.infoDigest
	r.expectedIndexDigest = fsDigests.indexDigest

	return nil
}

func hashID(id []byte) uint64 { return xxhash.Sum64(id) }

func (r *summarizer) readInfo(size int) error {
	buf := make([]byte, size)
	n, err := r.infoFdWithDigest.ReadAllAndValidate(buf, r.expectedInfoDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(msgpack.NewByteDecoderStream(buf[:n]))
	info, err := r.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}
	r.start = xtime.FromNanoseconds(info.BlockStart)
	r.volume = info.VolumeIndex
	r.blockSize = time.Duration(info.BlockSize)
	r.entries = int(info.Entries)
	r.metadataRead = 0
	return nil
}

func (r *summarizer) readIndexAndSortByOffsetAsc() error {
	if r.orderedByIndex {
		return errUnexpectedSortByOffset
	}

	r.decoder.Reset(r.indexDecoderStream)
	for i := 0; i < r.entries; i++ {
		entry, err := r.decoder.DecodeIndexEntry(nil)
		if err != nil {
			return err
		}
		r.indexEntriesByOffsetAsc = append(r.indexEntriesByOffsetAsc, entry)
	}
	// NB(r): As we decode each block we need access to each index entry
	// in the order we decode the data
	sort.Sort(indexEntriesByOffsetAsc(r.indexEntriesByOffsetAsc))
	return nil
}

func (r *summarizer) ReadMetadata() (IndexSummary, error) {
	entry, err := r.decoder.DecodeIndexEntry(nil)
	if err != nil {
		return IndexSummary{}, err
	}

	r.metadataRead++
	return IndexSummary{
		IDHash:       hashID(entry.ID),
		DataChecksum: entry.DataChecksum,
	}, nil
}

// NB(r): ValidateMetadata can be called immediately after Open(...) since
// the metadata is read upfront.
func (r *summarizer) ValidateMetadata() error {
	err := r.indexDecoderStream.reader().Validate(r.expectedIndexDigest)
	if err != nil {
		return fmt.Errorf("could not validate index file: %v", err)
	}
	return nil
}

func (r *summarizer) Range() xtime.Range {
	return xtime.Range{Start: r.start, End: r.start.Add(r.blockSize)}
}

func (r *summarizer) Entries() int {
	return r.entries
}

func (r *summarizer) MetadataRead() int {
	return r.metadataRead
}

func (r *summarizer) Close() error {
	// Close and prepare resources that are to be reused
	multiErr := xerrors.NewMultiError()
	multiErr = multiErr.Add(mmap.Munmap(r.indexMmap))
	multiErr = multiErr.Add(r.indexFd.Close())
	r.indexDecoderStream.Reset(nil)
	for i := 0; i < len(r.indexEntriesByOffsetAsc); i++ {
		r.indexEntriesByOffsetAsc[i].ID = nil
	}
	r.indexEntriesByOffsetAsc = r.indexEntriesByOffsetAsc[:0]

	// Save fields we want to reassign after resetting struct
	opts := r.opts
	filePathPrefix := r.filePathPrefix
	hugePagesOpts := r.hugePagesOpts
	infoFdWithDigest := r.infoFdWithDigest
	digestFdWithDigestContents := r.digestFdWithDigestContents
	indexDecoderStream := r.indexDecoderStream
	decoder := r.decoder
	digestBuf := r.digestBuf
	indexEntriesByOffsetAsc := r.indexEntriesByOffsetAsc

	// Reset struct
	*r = summarizer{}

	// Reset the saved fields
	r.opts = opts
	r.filePathPrefix = filePathPrefix
	r.hugePagesOpts = hugePagesOpts
	r.infoFdWithDigest = infoFdWithDigest
	r.digestFdWithDigestContents = digestFdWithDigestContents
	r.indexDecoderStream = indexDecoderStream
	r.decoder = decoder
	r.digestBuf = digestBuf
	r.indexEntriesByOffsetAsc = indexEntriesByOffsetAsc

	return multiErr.FinalError()
}
