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
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"
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
)

var (
	errCommitLogWriterAlreadyOpen                = errors.New("commit log writer already open")
	errCommitLogWriterFlushWithoutReservedLength = errors.New("commit log writer flushed without header reserve")

	endianness = binary.LittleEndian
)

type commitLogWriter interface {
	// Open opens the commit log for writing data
	Open(start time.Time, duration time.Duration) error

	// Write will write an entry in the commit log for a given series
	Write(
		series Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error

	// Flush will flush the contents to the disk, useful when first testing if first commit log is writable
	Flush() error

	// Close the reader
	Close() error
}

type flushFn func(err error)

type writer struct {
	filePathPrefix     string
	newFileMode        os.FileMode
	newDirectoryMode   os.FileMode
	nowFn              clock.NowFn
	bitset             bitset
	start              time.Time
	duration           time.Duration
	chunkWriter        *chunkWriter
	chunkReserveHeader []byte
	buffer             *bufio.Writer
	sizeBuffer         []byte
	logEncoder         encoding.Encoder
	metadataEncoder    encoding.Encoder
}

func newCommitLogWriter(
	flushFn flushFn,
	opts Options,
) commitLogWriter {
	return &writer{
		filePathPrefix:     opts.FilesystemOptions().FilePathPrefix(),
		newFileMode:        opts.FilesystemOptions().NewFileMode(),
		newDirectoryMode:   opts.FilesystemOptions().NewDirectoryMode(),
		nowFn:              opts.ClockOptions().NowFn(),
		chunkWriter:        newChunkWriter(flushFn),
		chunkReserveHeader: make([]byte, chunkHeaderLen),
		buffer:             bufio.NewWriterSize(nil, opts.FlushSize()),
		bitset:             newBitset(),
		sizeBuffer:         make([]byte, binary.MaxVarintLen64),
		logEncoder:         msgpack.NewEncoder(),
		metadataEncoder:    msgpack.NewEncoder(),
	}
}

func (w *writer) Open(start time.Time, duration time.Duration) error {
	if w.isOpen() {
		return errCommitLogWriterAlreadyOpen
	}

	commitLogsDir := fs.CommitLogsDirPath(w.filePathPrefix)
	if err := os.MkdirAll(commitLogsDir, w.newDirectoryMode); err != nil {
		return err
	}

	filePath, index := fs.NextCommitLogsFile(w.filePathPrefix, start)
	logInfo := schema.LogInfo{
		Start:    start.UnixNano(),
		Duration: int64(duration),
		Index:    int64(index),
	}
	w.logEncoder.Reset()
	if err := w.logEncoder.EncodeLogInfo(logInfo); err != nil {
		return err
	}
	fd, err := fs.OpenWritable(filePath, w.newFileMode)
	if err != nil {
		return err
	}

	w.chunkWriter.fd = fd
	w.buffer.Reset(w.chunkWriter)
	if err := w.write(w.logEncoder.Bytes()); err != nil {
		w.Close()
		return err
	}

	w.start = start
	w.duration = duration
	return nil
}

func (w *writer) isOpen() bool {
	return w.chunkWriter.fd != nil
}

func (w *writer) Write(
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	var logEntry schema.LogEntry
	logEntry.Create = w.nowFn().UnixNano()
	logEntry.Index = series.UniqueIndex

	seen := w.bitset.has(logEntry.Index)
	if !seen {
		// If "idx" hasn't been written to commit log
		// yet we need to include series metadata
		var metadata schema.LogMetadata
		metadata.ID = series.ID.Data().Get()
		metadata.Namespace = series.Namespace.Data().Get()
		metadata.Shard = series.Shard
		w.metadataEncoder.Reset()
		if err := w.metadataEncoder.EncodeLogMetadata(metadata); err != nil {
			return err
		}
		logEntry.Metadata = w.metadataEncoder.Bytes()
	}

	logEntry.Timestamp = datapoint.Timestamp.UnixNano()
	logEntry.Value = datapoint.Value
	logEntry.Unit = uint32(unit)
	logEntry.Annotation = annotation
	w.logEncoder.Reset()
	if err := w.logEncoder.EncodeLogEntry(logEntry); err != nil {
		return err
	}
	if err := w.write(w.logEncoder.Bytes()); err != nil {
		return err
	}

	if !seen {
		// Record we have seen this series
		w.bitset.set(logEntry.Index)
	}
	return nil
}

func (w *writer) Flush() error {
	return w.buffer.Flush()
}

func (w *writer) Close() error {
	if !w.isOpen() {
		return nil
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := w.chunkWriter.fd.Close(); err != nil {
		return err
	}

	w.chunkWriter.fd = nil
	w.bitset.clearAll()
	w.start = timeZero
	w.duration = 0
	return nil
}

func (w *writer) write(data []byte) error {
	dataLen := len(data)
	sizeLen := binary.PutUvarint(w.sizeBuffer, uint64(dataLen))
	totalLen := sizeLen + dataLen

	// Avoid writing across the checksum boundary if we can avoid it
	if w.buffer.Buffered() > 0 && totalLen > w.buffer.Available() {
		if err := w.buffer.Flush(); err != nil {
			return err
		}
		return w.write(data)
	}

	// Write size and then data
	if _, err := w.buffer.Write(w.sizeBuffer[:sizeLen]); err != nil {
		return err
	}
	_, err := w.buffer.Write(data)
	return err
}

type chunkWriter struct {
	fd      *os.File
	flushFn flushFn
	header  []byte
}

func newChunkWriter(flushFn flushFn) *chunkWriter {
	return &chunkWriter{flushFn: flushFn, header: make([]byte, chunkHeaderLen)}
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	size := len(p)

	sizeStart, sizeEnd :=
		0, chunkHeaderSizeLen
	checksumSizeStart, checksumSizeEnd :=
		sizeEnd, sizeEnd+chunkHeaderSizeLen
	checksumDataStart, checksumDataEnd :=
		checksumSizeEnd, checksumSizeEnd+chunkHeaderChecksumDataLen

	// Write size
	endianness.PutUint32(w.header[sizeStart:sizeEnd], uint32(size))

	// Calculate checksums
	checksumSize := digest.Checksum(w.header[sizeStart:sizeEnd])
	checksumData := digest.Checksum(p)

	// Write checksums
	digest.
		Buffer(w.header[checksumSizeStart:checksumSizeEnd]).
		WriteDigest(checksumSize)
	digest.
		Buffer(w.header[checksumDataStart:checksumDataEnd]).
		WriteDigest(checksumData)

	// Write header to file descriptor
	if _, err := w.fd.Write(w.header); err != nil {
		// Fire flush callback
		w.flushFn(err)
		return 0, err
	}

	// Write contents to file descriptor
	n, err := w.fd.Write(p)

	// Fire flush callback
	w.flushFn(err)
	return n, err
}
