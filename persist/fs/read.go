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
	"io"
	"os"
	"sort"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	// errCheckpointFileNotFound returned when the checkpoint file doesn't exist
	errCheckpointFileNotFound = errors.New("checkpoint file does not exist")

	// errReadNotExpectedSize returned when the size of the next read does not match size specified by the index
	errReadNotExpectedSize = errors.New("next read not expected size")
)

type reader struct {
	opts           Options
	filePathPrefix string
	hugePagesOpts  mmapHugeTLBOptions
	start          time.Time
	blockSize      time.Duration

	infoFdWithDigest           digest.FdWithDigestReader
	bloomFilterWithDigest      digest.FdWithDigestReader
	digestFdWithDigestContents digest.FdWithDigestContentsReader

	indexFd                 *os.File
	indexMmap               []byte
	indexDecoderStream      filesetReaderDecoderStream
	indexEntriesByOffsetAsc []schema.IndexEntry

	dataFd     *os.File
	dataMmap   []byte
	dataReader digest.ReaderWithDigest

	bloomFilterFd *os.File

	expectedInfoDigest        uint32
	expectedIndexDigest       uint32
	expectedDataDigest        uint32
	expectedDigestOfDigest    uint32
	expectedBloomFilterDigest uint32
	entries                   int
	bloomFilterInfo           schema.IndexBloomFilterInfo
	entriesRead               int
	metadataRead              int
	decoder                   *msgpack.Decoder
	digestBuf                 digest.Buffer
	bytesPool                 pool.CheckedBytesPool
}

// NewReader returns a new reader and expects all files to exist. Will read the
// index info in full on call to Open. The bytesPool can be passed as nil if callers
// would prefer just dynamically allocated IDs and data.
func NewReader(
	bytesPool pool.CheckedBytesPool,
	opts Options,
) (FileSetReader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &reader{
		opts:           opts,
		filePathPrefix: opts.FilePathPrefix(),
		hugePagesOpts: mmapHugeTLBOptions{
			enabled:   opts.MmapEnableHugeTLB(),
			threshold: opts.MmapHugeTLBThreshold(),
		},
		infoFdWithDigest:           digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(opts.InfoReaderBufferSize()),
		bloomFilterWithDigest:      digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		indexDecoderStream:         newReaderDecoderStream(),
		dataReader:                 digest.NewReaderWithDigest(nil),
		decoder:                    msgpack.NewDecoder(opts.DecodingOptions()),
		digestBuf:                  digest.NewBuffer(),
		bytesPool:                  bytesPool,
	}, nil
}

func (r *reader) Open(namespace ts.ID, shard uint32, blockStart time.Time) error {
	var err error

	// If there is no checkpoint file, don't read the data files.
	shardDir := ShardDirPath(r.filePathPrefix, namespace, shard)
	if err := r.readCheckpointFile(shardDir, blockStart); err != nil {
		return err
	}

	var infoFd, digestFd *os.File
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):        &infoFd,
		filesetPathFromTime(shardDir, blockStart, digestFileSuffix):      &digestFd,
		filesetPathFromTime(shardDir, blockStart, bloomFilterFileSuffix): &r.bloomFilterFd,
	}); err != nil {
		return err
	}

	r.infoFdWithDigest.Reset(infoFd)
	r.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		r.infoFdWithDigest.Close()
		r.digestFdWithDigestContents.Close()
	}()

	result, err := mmapFiles(os.Open, map[string]mmapFileDesc{
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix): mmapFileDesc{
			file:    &r.indexFd,
			bytes:   &r.indexMmap,
			options: mmapOptions{read: true, hugeTLB: r.hugePagesOpts},
		},
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix): mmapFileDesc{
			file:    &r.dataFd,
			bytes:   &r.dataMmap,
			options: mmapOptions{read: true, hugeTLB: r.hugePagesOpts},
		},
	})
	if err != nil {
		return err
	}
	if warning := result.warning; warning != nil {
		r.opts.InstrumentOptions().Logger().Warnf(
			"warning while mmapping files in reader: %s", warning.Error())
	}

	r.indexDecoderStream.Reset(r.indexMmap)
	r.dataReader.Reset(bytes.NewReader(r.dataMmap))

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
	if err := r.readIndex(); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *reader) readCheckpointFile(shardDir string, blockStart time.Time) error {
	filePath := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	if !FileExists(filePath) {
		return errCheckpointFileNotFound
	}
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()
	digest, err := r.digestBuf.ReadDigestFromFile(fd)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest
	return nil
}

func (r *reader) readDigest() error {
	fsDigests, err := readFilesetDigests(r.digestFdWithDigestContents)
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
	r.expectedBloomFilterDigest = fsDigests.bloomFilterDigest
	r.expectedDataDigest = fsDigests.dataDigest

	return nil
}

func (r *reader) readInfo(size int) error {
	buf := make([]byte, size)
	n, err := r.infoFdWithDigest.ReadAllAndValidate(buf, r.expectedInfoDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(msgpack.NewDecoderStream(buf[:n]))
	info, err := r.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}
	r.start = xtime.FromNanoseconds(info.Start)
	r.blockSize = time.Duration(info.BlockSize)
	r.entries = int(info.Entries)
	r.entriesRead = 0
	r.metadataRead = 0
	r.bloomFilterInfo = info.BloomFilter
	return nil
}

func (r *reader) readIndex() error {
	r.decoder.Reset(r.indexDecoderStream)
	for i := 0; i < r.entries; i++ {
		entry, err := r.decoder.DecodeIndexEntry()
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

func (r *reader) Read() (ts.ID, checked.Bytes, uint32, error) {
	var none ts.ID
	if r.entriesRead >= r.entries {
		return none, nil, 0, io.EOF
	}

	entry := r.indexEntriesByOffsetAsc[r.entriesRead]

	var data checked.Bytes
	if r.bytesPool != nil {
		data = r.bytesPool.Get(int(entry.Size))
		data.IncRef()
		defer data.DecRef()
		data.Resize(int(entry.Size))
	} else {
		data = checked.NewBytes(make([]byte, entry.Size), nil)
		data.IncRef()
		defer data.DecRef()
	}

	n, err := r.dataReader.Read(data.Get())
	if err != nil {
		return none, nil, 0, err
	}
	if n != int(entry.Size) {
		return none, nil, 0, errReadNotExpectedSize
	}

	r.entriesRead++

	return r.entryID(entry.ID), data, uint32(entry.Checksum), nil
}

func (r *reader) ReadMetadata() (id ts.ID, length int, checksum uint32, err error) {
	var none ts.ID
	if r.metadataRead >= r.entries {
		return none, 0, 0, io.EOF
	}

	entry := r.indexEntriesByOffsetAsc[r.metadataRead]
	r.metadataRead++
	return r.entryID(entry.ID), int(entry.Size), uint32(entry.Checksum), nil
}

func (r *reader) ReadBloomFilter() (*ManagedConcurrentBloomFilter, error) {
	return newManagedConcurrentBloomFilterFromFile(
		r.bloomFilterFd,
		r.bloomFilterWithDigest,
		r.expectedBloomFilterDigest,
		uint(r.bloomFilterInfo.NumElementsM),
		uint(r.bloomFilterInfo.NumHashesK),
	)
}

func (r *reader) entryID(id []byte) ts.ID {
	var idClone checked.Bytes
	if r.bytesPool != nil {
		idClone = r.bytesPool.Get(len(id))
		idClone.IncRef()
		defer idClone.DecRef()
	} else {
		idClone = checked.NewBytes(make([]byte, 0, len(id)), nil)
		idClone.IncRef()
		defer idClone.DecRef()
	}

	idClone.AppendAll(id)

	return ts.BinaryID(idClone)
}

// NB(xichen): Validate should be called after all data are read because the
// digest is calculated for the entire data file.
func (r *reader) Validate() error {
	err := r.indexDecoderStream.reader().Validate(r.expectedIndexDigest)
	if err != nil {
		return fmt.Errorf("could not validate index file: %v", err)
	}
	// Note that the seeker must validate the checksum since we don't always
	// verify the data file if we don't read it on bootstrap and call ReadMetadata instead.
	if r.entriesRead == 0 {
		return nil // Haven't read the records just the IDs
	}
	err = r.dataReader.Validate(r.expectedDataDigest)
	if err != nil {
		return fmt.Errorf("could not validate data file: %v", err)
	}
	return nil
}

func (r *reader) Range() xtime.Range {
	return xtime.Range{Start: r.start, End: r.start.Add(r.blockSize)}
}

func (r *reader) Entries() int {
	return r.entries
}

func (r *reader) EntriesRead() int {
	return r.entriesRead
}

func (r *reader) Close() error {
	for i := 0; i < len(r.indexEntriesByOffsetAsc); i++ {
		r.indexEntriesByOffsetAsc[i].ID = nil
	}
	r.indexEntriesByOffsetAsc = r.indexEntriesByOffsetAsc[:0]

	multiErr := xerrors.NewMultiError()

	multiErr = multiErr.Add(munmap(r.indexMmap))
	r.indexMmap = nil

	multiErr = multiErr.Add(munmap(r.dataMmap))
	r.dataMmap = nil

	multiErr = multiErr.Add(r.indexFd.Close())
	r.indexFd = nil

	multiErr = multiErr.Add(r.dataFd.Close())
	r.dataFd = nil

	multiErr = multiErr.Add(r.bloomFilterFd.Close())
	r.bloomFilterFd = nil

	return multiErr.FinalError()
}

// indexEntriesByOffsetAsc implements sort.Sort
type indexEntriesByOffsetAsc []schema.IndexEntry

func (e indexEntriesByOffsetAsc) Len() int {
	return len(e)
}

func (e indexEntriesByOffsetAsc) Less(i, j int) bool {
	return e[i].Offset < e[j].Offset
}

func (e indexEntriesByOffsetAsc) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
