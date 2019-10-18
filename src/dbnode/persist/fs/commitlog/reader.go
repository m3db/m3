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

package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	// var instead of const so we can modify them in tests.
	defaultDecodeEntryBufSize = 1024
	decoderInBufChanSize      = 1000
	decoderOutBufChanSize     = 1000
)

var (
	emptyLogInfo schema.LogInfo

	errCommitLogReaderChunkSizeChecksumMismatch = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderIsNotReusable             = errors.New("commit log reader is not reusable")
	errCommitLogReaderMultipleReadloops         = errors.New("commit log reader tried to open multiple readLoops, do not call Read() concurrently")
	errCommitLogReaderMissingMetadata           = errors.New("commit log reader encountered a datapoint without corresponding metadata")
)

// ReadAllSeriesPredicate can be passed as the seriesPredicate for callers
// that want a convenient way to read all series in the commitlogs
func ReadAllSeriesPredicate() SeriesFilterPredicate {
	return readAllSeriesFn
}

func readAllSeriesFn(id ident.ID, namespace ident.ID) bool {
	return true
}

type seriesMetadata struct {
	ts.Series
	passedPredicate bool
}

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (int64, error)

	// Read returns the next id and data pair or error, will return io.EOF at end of volume
	Read() (ts.Series, ts.Datapoint, xtime.Unit, ts.Annotation, error)

	// Close the reader
	Close() error
}

type reader struct {
	opts Options

	seriesPredicate SeriesFilterPredicate

	logEntryBytes          []byte
	tagDecoder             serialize.TagDecoder
	tagDecoderCheckedBytes checked.Bytes
	checkedBytesPool       pool.CheckedBytesPool
	chunkReader            *chunkReader
	infoDecoder            *msgpack.Decoder
	infoDecoderStream      msgpack.ByteDecoderStream
	hasBeenOpened          bool

	metadataLookup map[uint64]seriesMetadata
	namespacesRead []ident.ID
}

func newCommitLogReader(opts Options, seriesPredicate SeriesFilterPredicate) commitLogReader {
	tagDecoderCheckedBytes := checked.NewBytes(nil, nil)
	tagDecoderCheckedBytes.IncRef()
	return &reader{
		opts:                   opts,
		seriesPredicate:        seriesPredicate,
		logEntryBytes:          make([]byte, 0, opts.FlushSize()),
		metadataLookup:         make(map[uint64]seriesMetadata),
		tagDecoder:             opts.FilesystemOptions().TagDecoderPool().Get(),
		tagDecoderCheckedBytes: tagDecoderCheckedBytes,
		checkedBytesPool:       opts.BytesPool(),
		chunkReader:            newChunkReader(opts.FlushSize()),
		infoDecoder:            msgpack.NewDecoder(opts.FilesystemOptions().DecodingOptions()),
		infoDecoderStream:      msgpack.NewByteDecoderStream(nil),
	}
}

func (r *reader) Open(filePath string) (int64, error) {
	// Commitlog reader does not currently support being reused.
	if r.hasBeenOpened {
		return 0, errCommitLogReaderIsNotReusable
	}
	r.hasBeenOpened = true

	fd, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}

	r.chunkReader.reset(fd)
	info, err := r.readInfo()
	if err != nil {
		r.Close()
		return 0, err
	}
	index := info.Index
	return index, nil
}

func (r *reader) readInfo() (schema.LogInfo, error) {
	err := r.readLogEntry()
	if err != nil {
		return emptyLogInfo, err
	}
	r.infoDecoderStream.Reset(r.logEntryBytes)
	r.infoDecoder.Reset(r.infoDecoderStream)
	return r.infoDecoder.DecodeLogInfo()
}

// Read reads the next log entry in order.
func (r *reader) Read() (
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
	err error,
) {
	var (
		entry    schema.LogEntry
		metadata seriesMetadata
	)
	for !metadata.passedPredicate {
		err = r.readLogEntry()
		if err != nil {
			return ts.Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), err
		}

		entry, err = msgpack.DecodeLogEntryFast(r.logEntryBytes)
		if err != nil {
			return ts.Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), err
		}

		metadata, err = r.seriesMetadataForEntry(entry)
		if err != nil {
			return ts.Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), err
		}
	}

	series = metadata.Series

	datapoint = ts.Datapoint{
		Timestamp: time.Unix(0, entry.Timestamp),
		Value:     entry.Value,
	}

	unit = xtime.Unit(entry.Unit)

	if len(entry.Annotation) > 0 {
		// Copy annotation to prevent reference to pooled byte slice
		annotation = append(ts.Annotation(nil), ts.Annotation(entry.Annotation)...)
	}

	return series, datapoint, unit, annotation, nil
}

func (r *reader) readLogEntry() error {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return err
	}

	// Extend buffer as necessary
	r.logEntryBytes = resizeBufferOrGrowIfNeeded(r.logEntryBytes, int(size))

	// Read message
	if _, err := io.ReadFull(r.chunkReader, r.logEntryBytes); err != nil {
		return err
	}

	return nil
}

func (r *reader) seriesMetadataForEntry(
	entry schema.LogEntry,
) (seriesMetadata, error) {
	metadata, ok := r.metadataLookup[entry.Index]
	if ok {
		// If the metadata already exists, we can skip this step.
		return metadata, nil
	}

	if len(entry.Metadata) == 0 {
		// Expected metadata but not encoded.
		return seriesMetadata{}, errCommitLogReaderMissingMetadata
	}

	decoded, err := msgpack.DecodeLogMetadataFast(entry.Metadata)
	if err != nil {
		return seriesMetadata{}, err
	}

	id := r.checkedBytesPool.Get(len(decoded.ID))
	id.IncRef()
	id.AppendAll(decoded.ID)

	// Find or allocate the namespace ID.
	var namespaceID ident.ID
	for _, ns := range r.namespacesRead {
		if bytes.Equal(ns.Bytes(), decoded.Namespace) {
			namespaceID = ns
			break
		}
	}
	if namespaceID == nil {
		// Take a copy and keep around to reuse later.
		namespaceID = ident.BytesID(append([]byte(nil), decoded.Namespace...))
		r.namespacesRead = append(r.namespacesRead, namespaceID)
	}

	var (
		idPool      = r.opts.IdentifierPool()
		tags        ident.Tags
		tagBytesLen = len(decoded.EncodedTags)
	)
	if tagBytesLen != 0 {
		r.tagDecoderCheckedBytes.Reset(decoded.EncodedTags)
		r.tagDecoder.Reset(r.tagDecoderCheckedBytes)

		tags = idPool.Tags()
		for r.tagDecoder.Next() {
			curr := r.tagDecoder.Current()
			tags.Append(idPool.CloneTag(curr))
		}
		err = r.tagDecoder.Err()
		if err != nil {
			return seriesMetadata{}, err
		}
	}

	seriesID := idPool.BinaryID(id)
	metadata = seriesMetadata{
		Series: ts.Series{
			UniqueIndex: entry.Index,
			ID:          seriesID,
			Namespace:   namespaceID,
			Shard:       decoded.Shard,
			Tags:        tags,
		},
		passedPredicate: r.seriesPredicate(seriesID, namespaceID),
	}

	r.metadataLookup[entry.Index] = metadata

	id.DecRef()

	return metadata, nil
}

func (r *reader) Close() error {
	return r.chunkReader.fd.Close()
}

func resizeBufferOrGrowIfNeeded(buf []byte, length int) []byte {
	if cap(buf) >= length {
		return buf[:length]
	}

	// If double is less than length requested, return that.
	var newCap int
	for newCap = 2 * cap(buf); newCap < length; newCap *= 2 {
		// Double it again.
	}

	return make([]byte, length, newCap)
}
