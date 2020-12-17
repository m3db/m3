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

	"go.uber.org/atomic"
)

var (
	commitLogFileReadCounter = atomic.NewUint64(0)

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

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (int64, error)

	// Read returns the next id and data pair or error, will return io.EOF at end of volume
	Read() (LogEntry, error)

	// Close the reader
	Close() error
}

type reader struct {
	opts commitLogReaderOptions

	logEntryBytes          []byte
	tagDecoder             serialize.TagDecoder
	tagDecoderCheckedBytes checked.Bytes
	checkedBytesPool       pool.CheckedBytesPool
	chunkReader            *chunkReader
	infoDecoder            *msgpack.Decoder
	infoDecoderStream      msgpack.ByteDecoderStream
	hasBeenOpened          bool
	fileReadID             uint64

	metadataLookup map[uint64]ts.Series
	namespacesRead []namespaceRead
	seriesIDReused *ident.ReusableBytesID
}

type namespaceRead struct {
	namespaceIDBytes []byte
	namespaceIDRef   ident.ID
}

type commitLogReaderOptions struct {
	commitLogOptions Options
	// returnMetadataAsRef indicates to not allocate metadata results.
	returnMetadataAsRef bool
}

func newCommitLogReader(opts commitLogReaderOptions) commitLogReader {
	tagDecoderCheckedBytes := checked.NewBytes(nil, nil)
	tagDecoderCheckedBytes.IncRef()
	return &reader{
		opts:                   opts,
		logEntryBytes:          make([]byte, 0, opts.commitLogOptions.FlushSize()),
		metadataLookup:         make(map[uint64]ts.Series),
		tagDecoder:             opts.commitLogOptions.FilesystemOptions().TagDecoderPool().Get(),
		tagDecoderCheckedBytes: tagDecoderCheckedBytes,
		checkedBytesPool:       opts.commitLogOptions.BytesPool(),
		chunkReader:            newChunkReader(opts.commitLogOptions.FlushSize()),
		infoDecoder:            msgpack.NewDecoder(opts.commitLogOptions.FilesystemOptions().DecodingOptions()),
		infoDecoderStream:      msgpack.NewByteDecoderStream(nil),
		seriesIDReused:         ident.NewReusableBytesID(),
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

	r.fileReadID = commitLogFileReadCounter.Inc()

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
func (r *reader) Read() (LogEntry, error) {
	err := r.readLogEntry()
	if err != nil {
		return LogEntry{}, err
	}

	entry, err := msgpack.DecodeLogEntryFast(r.logEntryBytes)
	if err != nil {
		return LogEntry{}, err
	}

	metadata, err := r.seriesMetadataForEntry(entry)
	if err != nil {
		return LogEntry{}, err
	}

	result := LogEntry{
		Series: metadata,
		Datapoint: ts.Datapoint{
			Timestamp:      time.Unix(0, entry.Timestamp),
			TimestampNanos: xtime.UnixNano(entry.Timestamp),
			Value:          entry.Value,
		},
		Unit: xtime.Unit(entry.Unit),
		Metadata: LogEntryMetadata{
			FileReadID:        r.fileReadID,
			SeriesUniqueIndex: entry.Index,
		},
	}

	if len(entry.Annotation) > 0 {
		// Copy annotation to prevent reference to pooled byte slice
		result.Annotation = append(ts.Annotation(nil), ts.Annotation(entry.Annotation)...)
	}

	return result, nil
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

func (r *reader) namespaceIDReused(id []byte) ident.ID {
	var namespaceID ident.ID
	for _, ns := range r.namespacesRead {
		if bytes.Equal(ns.namespaceIDBytes, id) {
			namespaceID = ns.namespaceIDRef
			break
		}
	}
	if namespaceID == nil {
		// Take a copy and keep around to reuse later.
		namespaceBytes := append(make([]byte, 0, len(id)), id...)
		namespaceID = ident.BytesID(namespaceBytes)
		r.namespacesRead = append(r.namespacesRead, namespaceRead{
			namespaceIDBytes: namespaceBytes,
			namespaceIDRef:   namespaceID,
		})
	}
	return namespaceID
}

func (r *reader) seriesMetadataForEntry(
	entry schema.LogEntry,
) (ts.Series, error) {
	if r.opts.returnMetadataAsRef {
		// NB(r): This is a fast path for callers where nothing
		// is cached locally in terms of metadata lookup and the
		// caller is returned just references to all the bytes in
		// the backing commit log file the first and only time
		// we encounter the series metadata, and then the refs are
		// invalid on the next call to metadata.
		if len(entry.Metadata) == 0 {
			// Valid, nothing to return here and caller will already
			// have processed metadata for this entry (based on the
			// FileReadID and the SeriesUniqueIndex returned).
			return ts.Series{}, nil
		}

		decoded, err := msgpack.DecodeLogMetadataFast(entry.Metadata)
		if err != nil {
			return ts.Series{}, err
		}

		// Reset the series ID being returned.
		r.seriesIDReused.Reset(decoded.ID)
		// Find or allocate the namespace ID.
		namespaceID := r.namespaceIDReused(decoded.Namespace)
		metadata := ts.Series{
			UniqueIndex: entry.Index,
			ID:          r.seriesIDReused,
			Namespace:   namespaceID,
			Shard:       decoded.Shard,
			EncodedTags: ts.EncodedTags(decoded.EncodedTags),
		}
		return metadata, nil
	}

	// We only check for previously returned metadata
	// if we're allocating results and can hold onto them.
	metadata, ok := r.metadataLookup[entry.Index]
	if ok {
		// If the metadata already exists, we can skip this step.
		return metadata, nil
	}

	if len(entry.Metadata) == 0 {
		// Expected metadata but not encoded.
		return ts.Series{}, errCommitLogReaderMissingMetadata
	}

	decoded, err := msgpack.DecodeLogMetadataFast(entry.Metadata)
	if err != nil {
		return ts.Series{}, err
	}

	id := r.checkedBytesPool.Get(len(decoded.ID))
	id.IncRef()
	id.AppendAll(decoded.ID)

	// Find or allocate the namespace ID.
	namespaceID := r.namespaceIDReused(decoded.Namespace)

	// Need to copy encoded tags since will be invalid when
	// progressing to next record.
	encodedTags := append(
		make([]byte, 0, len(decoded.EncodedTags)),
		decoded.EncodedTags...)

	idPool := r.opts.commitLogOptions.IdentifierPool()
	metadata = ts.Series{
		UniqueIndex: entry.Index,
		ID:          idPool.BinaryID(id),
		Namespace:   namespaceID,
		Shard:       decoded.Shard,
		EncodedTags: ts.EncodedTags(encodedTags),
	}

	r.metadataLookup[entry.Index] = metadata

	id.DecRef()

	return metadata, nil
}

func (r *reader) Close() error {
	err := r.chunkReader.fd.Close()
	// NB(r): Reset to free resources, but explicitly do
	// not support reopening for now.
	*r = reader{}
	r.hasBeenOpened = true
	return err
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
