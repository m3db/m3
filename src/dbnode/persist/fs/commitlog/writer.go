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
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/m3db/bitset"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// The lengths to reserve for a chunk header:
	// - size uint32
	// - checksumSize uint32
	// - checksumData uint32
	chunkHeaderSizeLen         = 4
	chunkHeaderChecksumSizeLen = 4
	chunkHeaderChecksumDataLen = 4
	chunkHeaderLen             = chunkHeaderSizeLen +
		chunkHeaderChecksumSizeLen +
		chunkHeaderChecksumDataLen

	defaultBitSetLength = 65536

	defaultEncoderBuffSize = 16384
)

var (
	errCommitLogWriterAlreadyOpen = errors.New("commit log writer already open")
	errTagEncoderDataNotAvailable = errors.New("tag iterator data not available")

	endianness = binary.LittleEndian
)

type commitLogWriter interface {
	// Open opens the commit log for writing data
	Open() (persist.CommitLogFile, error)

	// Write will write an entry in the commit log for a given series
	Write(
		series ts.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
		callbackFn callbackFn,
	) error

	// Flush will flush any data in the writers buffer to the chunkWriter, essentially forcing
	// a new chunk to be created. Optionally forces the data to be FSync'd to disk.
	Flush(sync bool) error

	// Close the reader
	Close() error
}

type chunkWriter interface {
	io.Writer

	reset(f xos.File)
	close() error
	isOpen() bool
}

type flushFn func(err error)

type writer struct {
	filePathPrefix      string
	newFileMode         os.FileMode
	newDirectoryMode    os.FileMode
	nowFn               clock.NowFn
	chunkWriter         chunkWriter
	chunkReserveHeader  []byte
	buffer              *asyncFlushBuffer
	sizeBuffer          []byte
	seen                *bitset.BitSet
	logEncoder          *msgpack.Encoder
	logEncoderBuff      []byte
	metadataEncoderBuff []byte
	tagEncoder          serialize.TagEncoder
	tagSliceIter        ident.TagsIterator
	opts                Options
}

func newCommitLogWriter(
	flushFn flushFn,
	opts Options,
) commitLogWriter {
	shouldFsync := opts.Strategy() == StrategyWriteWait

	flushSize := opts.FlushSize()
	flushBufferSize := opts.FlushBufferSize()

	// Use 10mb flush size, and 200mb flush buffer for testing.
	flushSize = 10 * 1024 * 1024
	flushBufferSize = 200 * 1024 * 1024

	// Allow for at least 2 total buffers.
	if 2*flushSize > flushBufferSize {
		flushBufferSize = 2 * flushSize
	}

	chunkWriter := newChunkWriter(flushFn, shouldFsync)
	buffer := newAsyncFlushBuffer(chunkWriter, asyncFlushBufferOptions{
		totalBufferLength:  flushBufferSize,
		singleBufferLength: flushSize,
	})
	return &writer{
		filePathPrefix:      opts.FilesystemOptions().FilePathPrefix(),
		newFileMode:         opts.FilesystemOptions().NewFileMode(),
		newDirectoryMode:    opts.FilesystemOptions().NewDirectoryMode(),
		nowFn:               opts.ClockOptions().NowFn(),
		chunkWriter:         chunkWriter,
		chunkReserveHeader:  make([]byte, chunkHeaderLen),
		buffer:              buffer,
		sizeBuffer:          make([]byte, binary.MaxVarintLen64),
		seen:                bitset.NewBitSet(defaultBitSetLength),
		logEncoder:          msgpack.NewEncoder(),
		logEncoderBuff:      make([]byte, 0, defaultEncoderBuffSize),
		metadataEncoderBuff: make([]byte, 0, defaultEncoderBuffSize),
		tagEncoder:          opts.FilesystemOptions().TagEncoderPool().Get(),
		tagSliceIter:        ident.NewTagsIterator(ident.Tags{}),
		opts:                opts,
	}
}

func (w *writer) Open() (persist.CommitLogFile, error) {
	if w.isOpen() {
		return persist.CommitLogFile{}, errCommitLogWriterAlreadyOpen
	}

	// Reset buffers since they will grow 2x on demand so we want to make sure that
	// one exceptionally large write does not cause them to remain oversized forever.
	if cap(w.logEncoderBuff) != defaultEncoderBuffSize {
		w.logEncoderBuff = make([]byte, 0, defaultEncoderBuffSize)
	}
	if cap(w.metadataEncoderBuff) != defaultEncoderBuffSize {
		w.metadataEncoderBuff = make([]byte, 0, defaultEncoderBuffSize)
	}

	commitLogsDir := fs.CommitLogsDirPath(w.filePathPrefix)
	if err := os.MkdirAll(commitLogsDir, w.newDirectoryMode); err != nil {
		return persist.CommitLogFile{}, err
	}

	filePath, index, err := NextFile(w.opts)
	if err != nil {
		return persist.CommitLogFile{}, err
	}
	logInfo := schema.LogInfo{
		Index: int64(index),
	}
	w.logEncoder.Reset()
	if err := w.logEncoder.EncodeLogInfo(logInfo); err != nil {
		return persist.CommitLogFile{}, err
	}
	fd, err := fs.OpenWritable(filePath, w.newFileMode)
	if err != nil {
		return persist.CommitLogFile{}, err
	}

	w.chunkWriter.reset(fd)
	w.buffer.open()
	if err := w.write(w.logEncoder.Bytes(), nil); err != nil {
		w.Close()
		return persist.CommitLogFile{}, err
	}

	return persist.CommitLogFile{
		FilePath: filePath,
		Index:    int64(index),
	}, nil
}

func (w *writer) isOpen() bool {
	return w.chunkWriter.isOpen()
}

func (w *writer) Write(
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
	callbackFn callbackFn,
) error {
	var logEntry schema.LogEntry
	logEntry.Create = w.nowFn().UnixNano()
	logEntry.Index = series.UniqueIndex

	seen := w.seen.Test(uint(series.UniqueIndex))
	if !seen {
		var encodedTags []byte
		if series.EncodedTags != nil {
			// If already serialized use the serialized tags.
			encodedTags = series.EncodedTags
		} else if series.Tags.Values() != nil {
			// Otherwise serialize the tags.
			w.tagSliceIter.Reset(series.Tags)
			w.tagEncoder.Reset()
			err := w.tagEncoder.Encode(w.tagSliceIter)
			if err != nil {
				return err
			}

			encodedTagsChecked, ok := w.tagEncoder.Data()
			if !ok {
				return errTagEncoderDataNotAvailable
			}

			encodedTags = encodedTagsChecked.Bytes()
		}

		// If "idx" likely hasn't been written to commit log
		// yet we need to include series metadata
		var metadata schema.LogMetadata
		metadata.ID = series.ID.Bytes()
		metadata.Namespace = series.Namespace.Bytes()
		metadata.Shard = series.Shard
		metadata.EncodedTags = encodedTags

		var err error
		w.metadataEncoderBuff, err = msgpack.EncodeLogMetadataFast(w.metadataEncoderBuff[:0], metadata)
		if err != nil {
			return err
		}
		logEntry.Metadata = w.metadataEncoderBuff
	}

	logEntry.Timestamp = datapoint.Timestamp.UnixNano()
	logEntry.Value = datapoint.Value
	logEntry.Unit = uint32(unit)
	logEntry.Annotation = annotation

	var err error
	w.logEncoderBuff, err = msgpack.EncodeLogEntryFast(w.logEncoderBuff[:0], logEntry)
	if err != nil {
		return err
	}

	if err := w.write(w.logEncoderBuff, callbackFn); err != nil {
		return err
	}

	if !seen {
		// Record we have written this series and metadata to this commit log
		w.seen.Set(uint(series.UniqueIndex))
	}
	return nil
}

func (w *writer) Flush(sync bool) error {
	// Do not need to sync() if using WriteWait, since chunk writer
	// calls sync() then onFlush.
	if err := w.buffer.flush(); err != nil {
		return err
	}
	if !sync {
		return nil
	}
	return w.buffer.sync()
}

func (w *writer) Close() error {
	if !w.isOpen() {
		return nil
	}

	if err := w.buffer.close(); err != nil {
		return err
	}
	if err := w.chunkWriter.close(); err != nil {
		return err
	}

	w.seen.ClearAll()
	return nil
}

func (w *writer) write(data []byte, callbackFn callbackFn) error {
	dataLen := len(data)
	sizeLen := binary.PutUvarint(w.sizeBuffer, uint64(dataLen))

	// Write size and then data
	if err := w.buffer.write(w.sizeBuffer[:sizeLen], nil); err != nil {
		return err
	}
	return w.buffer.write(data, callbackFn)
}

type asyncFlushBuffer struct {
	writers            []asyncFlushBufferWriter
	singleBufferLength int
	idx                int
	flushCalls         chan asyncFlushBufferCall
	chunkWriter        chunkWriter
}

type asyncFlushBufferWriter struct {
	buffer    []byte
	callbacks []callbackFn
	result    *asyncFlushBufferCallResult
}

type asyncFlushBufferCallType uint

const (
	asyncFlushBufferFlushCall asyncFlushBufferCallType = iota
	asyncFlushBufferSyncCall
)

type asyncFlushBufferCall struct {
	bytes     []byte
	callbacks []callbackFn
	syncWg    *sync.WaitGroup
	callType  asyncFlushBufferCallType
	result    *asyncFlushBufferCallResult
}

type asyncFlushBufferCallResult struct {
	wg  *sync.WaitGroup
	n   int
	err error
}

func (r *asyncFlushBufferCallResult) reset() {
	r.n = 0
	r.err = nil
}

type asyncFlushBufferOptions struct {
	totalBufferLength  int
	singleBufferLength int
}

func newAsyncFlushBuffer(
	chunkWriter chunkWriter,
	opts asyncFlushBufferOptions,
) *asyncFlushBuffer {
	var (
		writers []asyncFlushBufferWriter
		total   int
	)
	for total+opts.singleBufferLength <= opts.totalBufferLength {
		writers = append(writers, asyncFlushBufferWriter{
			buffer: make([]byte, 0, opts.singleBufferLength),
			result: &asyncFlushBufferCallResult{
				wg: &sync.WaitGroup{},
			},
		})
		total += opts.singleBufferLength
	}

	return &asyncFlushBuffer{
		writers:            writers,
		singleBufferLength: opts.singleBufferLength,
		chunkWriter:        chunkWriter,
	}
}

func (b *asyncFlushBuffer) open() {
	b.flushCalls = make(chan asyncFlushBufferCall, len(b.writers))
	go b.flushLoop()
}

func (b *asyncFlushBuffer) flushLoop() {
	for flush := range b.flushCalls {
		switch flush.callType {
		case asyncFlushBufferFlushCall:
			flush.result.n, flush.result.err = b.chunkWriter.Write(flush.bytes)
			for _, fn := range flush.callbacks {
				fn(callbackResult{
					eventType: flushEventType,
					err:       flush.result.err,
				})
			}
			flush.result.wg.Done()
		case asyncFlushBufferSyncCall:
			flush.syncWg.Done()
		}
	}
}

func (b *asyncFlushBuffer) close() error {
	if err := b.flush(); err != nil {
		return err
	}
	err := b.sync()
	close(b.flushCalls)
	return err
}

func (b *asyncFlushBuffer) sync() error {
	var syncWg sync.WaitGroup
	syncWg.Add(1)

	// Enqueue sync.
	call := asyncFlushBufferCall{
		syncWg:   &syncWg,
		callType: asyncFlushBufferSyncCall,
	}
	b.flushCalls <- call

	// Wait for sync.
	syncWg.Wait()

	// Return any errors encountered.
	for _, elem := range b.writers {
		elem.result.wg.Wait()
		if err := elem.result.err; err != nil {
			return err
		}
	}

	return nil
}

func (b *asyncFlushBuffer) flush() error {
	if len(b.writers[b.idx].buffer) == 0 {
		return nil
	}
	return b.writeBuffer()
}

func (b *asyncFlushBuffer) write(p []byte, callbackFn callbackFn) error {
	pLen := len(p)
	currLen := len(b.writers[b.idx].buffer)
	newLen := currLen + pLen
	if currLen > 0 && newLen > b.singleBufferLength {
		// Flush current buffer.
		if err := b.writeBuffer(); err != nil {
			return err
		}
	}

	b.writers[b.idx].buffer = append(b.writers[b.idx].buffer, p...)
	if callbackFn != nil {
		b.writers[b.idx].callbacks = append(b.writers[b.idx].callbacks, callbackFn)
	}
	return nil
}

func (b *asyncFlushBuffer) writeBuffer() error {
	// First enqueue this buffer to be written.
	b.writers[b.idx].result.wg.Add(1)
	call := asyncFlushBufferCall{
		bytes:     b.writers[b.idx].buffer,
		callbacks: b.writers[b.idx].callbacks,
		callType:  asyncFlushBufferFlushCall,
		result:    b.writers[b.idx].result,
	}
	b.flushCalls <- call

	next := b.idx + 1
	if next >= len(b.writers) {
		next = 0 // Wrap around.
	}

	b.idx = next

	// Wait for next buffer to be ready.
	b.writers[b.idx].result.wg.Wait()

	// Save previous error.
	prevErr := b.writers[b.idx].result.err

	// Reset the buffer for reuse.
	b.writers[b.idx].buffer = b.writers[b.idx].buffer[:0]
	for i := range b.writers[b.idx].callbacks {
		b.writers[b.idx].callbacks[i] = nil
	}
	b.writers[b.idx].callbacks = b.writers[b.idx].callbacks[:0]
	b.writers[b.idx].result.reset()

	// Return the previous error, ensure we eventually propagate errors.
	return prevErr
}

type fsChunkWriter struct {
	fd      xos.File
	flushFn flushFn
	buff    []byte
	fsync   bool
}

func newChunkWriter(flushFn flushFn, fsync bool) chunkWriter {
	return &fsChunkWriter{
		flushFn: flushFn,
		buff:    make([]byte, chunkHeaderLen),
		fsync:   fsync,
	}
}

func (w *fsChunkWriter) reset(f xos.File) {
	w.fd = f
}

func (w *fsChunkWriter) close() error {
	err := w.fd.Close()
	w.fd = nil
	return err
}

func (w *fsChunkWriter) isOpen() bool {
	return w.fd != nil
}

// Writes a custom header in front of p to a file and returns number of bytes of p successfully written to the file.
// If the header or p is not fully written to the file, then this method returns number of bytes of p actually written
// to the file and an error explaining the reason of failure to write fully to the file.
func (w *fsChunkWriter) Write(p []byte) (int, error) {
	size := len(p)

	sizeStart, sizeEnd :=
		0, chunkHeaderSizeLen
	checksumSizeStart, checksumSizeEnd :=
		sizeEnd, sizeEnd+chunkHeaderSizeLen
	checksumDataStart, checksumDataEnd :=
		checksumSizeEnd, checksumSizeEnd+chunkHeaderChecksumDataLen

	// Write size
	endianness.PutUint32(w.buff[sizeStart:sizeEnd], uint32(size))

	// Calculate checksums
	checksumSize := digest.Checksum(w.buff[sizeStart:sizeEnd])
	checksumData := digest.Checksum(p)

	// Write checksums
	digest.
		Buffer(w.buff[checksumSizeStart:checksumSizeEnd]).
		WriteDigest(checksumSize)
	digest.
		Buffer(w.buff[checksumDataStart:checksumDataEnd]).
		WriteDigest(checksumData)

	// Combine buffers to reduce to a single syscall
	w.buff = append(w.buff[:chunkHeaderLen], p...)

	// Write contents to file descriptor
	n, err := w.fd.Write(w.buff)
	// Count bytes successfully written from slice p
	pBytesWritten := n - chunkHeaderLen
	if pBytesWritten < 0 {
		pBytesWritten = 0
	}

	if err != nil {
		w.flushFn(err)
		return pBytesWritten, err
	}

	// Fsync if required to
	if w.fsync {
		err = w.fd.Sync()
	}

	// Fire flush callback
	w.flushFn(err)
	return pBytesWritten, err
}
