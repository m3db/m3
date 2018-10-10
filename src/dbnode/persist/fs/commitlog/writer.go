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
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/m3db/bitset"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/ident"
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

	defaultBitSetLength = 65536
)

var (
	errCommitLogWriterAlreadyOpen = errors.New("commit log writer already open")
	errTagEncoderDataNotAvailable = errors.New("tag iterator data not available")

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

// type concurrentWriter struct {
// 	w *writer
// 	sync.Mutex
// }

// func (w *concurrentWriter) Open(start time.Time, duration time.Duration) error {
// 	w.Lock()
// 	defer w.Unlock()
// 	return w.w.Open(start, duration)
// }

// func (w *concurrentWriter) Write(
// 	series Series,
// 	datapoint ts.Datapoint,
// 	unit xtime.Unit,
// 	annotation ts.Annotation,
// ) error {
// 	w.Lock()
// 	defer w.Unlock()
// 	return w.w.Write(series, datapoint, unit, annotation)
// }

// func (w *concurrentWriter) write(p []byte) error {
// 	w.Lock()
// 	defer w.Unlock()
// 	return w.w.write(p)
// }

// func (w *concurrentWriter) Flush() error {
// 	w.Lock()
// 	defer w.Unlock()
// 	return w.w.Flush()
// }

// func (w *concurrentWriter) Close() error {
// 	w.Lock()
// 	defer w.Unlock()
// 	return w.w.Close()
// }

// type bufioWriterIface interface {
// 	Reset(w io.Writer)
// 	Flush() error
// 	Write(p []byte) (int, error)
// 	Available() int
// 	Buffered() int
// }

// type corruptingWriter struct {
// 	b *bufio.Writer
// 	sync.Mutex
// }

// func (c *corruptingWriter) Reset(w io.Writer) {
// 	c.Lock()
// 	defer c.Unlock()
// 	c.b.Reset(w)
// }

// func (c *corruptingWriter) Flush() error {
// 	c.Lock()
// 	defer c.Unlock()
// 	return c.b.Flush()
// }

// func (c *corruptingWriter) Write(p []byte) (int, error) {
// 	c.Lock()
// 	defer c.Unlock()
// 	if rand.Intn(100) <= 2 {
// 		var (
// 			byteStart  int
// 			byteOffset int
// 		)
// 		if len(p) > 1 {
// 			byteStart = rand.Intn(len(p) - 1)
// 			byteOffset = rand.Intn(len(p) - 1 - byteStart)
// 		}

// 		if byteStart >= 0 && byteStart+byteOffset < len(p) {
// 			copy(p[byteStart:byteStart+byteOffset], make([]byte, byteOffset))
// 		}
// 	}
// 	return c.b.Write(p)
// }

// func (c *corruptingWriter) Available() int {
// 	c.Lock()
// 	defer c.Unlock()
// 	return c.b.Available()
// }

// func (c *corruptingWriter) Buffered() int {
// 	c.Lock()
// 	defer c.Unlock()
// 	return c.b.Buffered()
// }

type fd interface {
	Sync() error
	Write(p []byte) (int, error)
	Close() error
}

type corruptingFd struct {
	f fd
}

func (c *corruptingFd) Sync() error {
	return c.f.Sync()
}

func (c *corruptingFd) Write(p []byte) (int, error) {
	if rand.Intn(100) <= 20 {
		var (
			byteStart  int
			byteOffset int
		)
		if len(p) > 1 {
			byteStart = rand.Intn(len(p) - 1)
			byteOffset = rand.Intn(len(p) - 1 - byteStart)
		}

		if byteStart >= 0 && byteStart+byteOffset < len(p) {
			copy(p[byteStart:byteStart+byteOffset], make([]byte, byteOffset))
		}
	}
	return c.f.Write(p)
}

func (c *corruptingFd) Close() error {
	return c.f.Close()
}

type chunkWriterIface interface {
	Reset(f fd)
	Write(p []byte) (int, error)
	Close() error
	IsOpen() bool
}

type corruptingChunkWriter struct {
	chunkWriter *chunkWriter
}

func (c *corruptingChunkWriter) Reset(f fd) {
	c.chunkWriter.fd = &corruptingFd{f}
}

func (c *corruptingChunkWriter) Write(p []byte) (int, error) {
	return c.chunkWriter.Write(p)
}

func (c *corruptingChunkWriter) Close() error {
	return c.chunkWriter.Close()
}

func (c *corruptingChunkWriter) IsOpen() bool {
	return c.chunkWriter.IsOpen()
}

type flushFn func(err error)

type writer struct {
	filePathPrefix     string
	newFileMode        os.FileMode
	newDirectoryMode   os.FileMode
	nowFn              clock.NowFn
	start              time.Time
	duration           time.Duration
	chunkWriter        chunkWriterIface
	chunkReserveHeader []byte
	buffer             *bufio.Writer
	sizeBuffer         []byte
	seen               *bitset.BitSet
	logEncoder         *msgpack.Encoder
	metadataEncoder    *msgpack.Encoder
	tagEncoder         serialize.TagEncoder
	tagSliceIter       ident.TagsIterator
}

func newCommitLogWriter(
	flushFn flushFn,
	opts Options,
) commitLogWriter {
	shouldFsync := opts.Strategy() == StrategyWriteWait

	return &writer{
		filePathPrefix:     opts.FilesystemOptions().FilePathPrefix(),
		newFileMode:        opts.FilesystemOptions().NewFileMode(),
		newDirectoryMode:   opts.FilesystemOptions().NewDirectoryMode(),
		nowFn:              opts.ClockOptions().NowFn(),
		chunkWriter:        newChunkWriter(flushFn, shouldFsync),
		chunkReserveHeader: make([]byte, chunkHeaderLen),
		buffer:             bufio.NewWriterSize(nil, opts.FlushSize()),
		sizeBuffer:         make([]byte, binary.MaxVarintLen64),
		seen:               bitset.NewBitSet(defaultBitSetLength),
		logEncoder:         msgpack.NewEncoder(),
		metadataEncoder:    msgpack.NewEncoder(),
		tagEncoder:         opts.FilesystemOptions().TagEncoderPool().Get(),
		tagSliceIter:       ident.NewTagsIterator(ident.Tags{}),
	}
}

func (w *writer) Open(start time.Time, duration time.Duration) error {
	if w.isOpen() {
		fmt.Println("w")
		return errCommitLogWriterAlreadyOpen
	}

	commitLogsDir := fs.CommitLogsDirPath(w.filePathPrefix)
	if err := os.MkdirAll(commitLogsDir, w.newDirectoryMode); err != nil {
		fmt.Println("wt")
		return err
	}
	fmt.Println("wtf")

	filePath, index, err := fs.NextCommitLogsFile(w.filePathPrefix, start)
	if err != nil {
		fmt.Println("wtf1")
		return err
	}
	logInfo := schema.LogInfo{
		Start:    start.UnixNano(),
		Duration: int64(duration),
		Index:    int64(index),
	}
	w.logEncoder.Reset()
	if err := w.logEncoder.EncodeLogInfo(logInfo); err != nil {
		fmt.Println("wtf2")
		return err
	}
	fmt.Println("wtf3")
	fd, err := fs.OpenWritable(filePath, w.newFileMode)
	if err != nil {
		fmt.Println("wtf4")
		return err
	}
	fmt.Println("wtf5")

	w.chunkWriter.Reset(fd)
	w.buffer.Reset(w.chunkWriter)
	if err := w.write(w.logEncoder.Bytes()); err != nil {
		fmt.Println("wtf6")
		w.Close()
		return err
	}

	w.start = start
	w.duration = duration
	return nil
}

func (w *writer) isOpen() bool {
	return w.chunkWriter.IsOpen()
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

	seen := w.seen.Test(uint(series.UniqueIndex))
	if !seen {
		var (
			tags        = series.Tags
			encodedTags []byte
		)

		if tags.Values() != nil {
			w.tagSliceIter.Reset(tags)
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
		// Record we have written this series and metadata to this commit log
		w.seen.Set(uint(series.UniqueIndex))
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

	fmt.Println(1)
	if err := w.Flush(); err != nil {
		fmt.Println(2)
		return err
	}
	fmt.Println(3)
	if err := w.chunkWriter.Close(); err != nil {
		fmt.Println(4)
		return err
	}
	fmt.Println(5)

	w.chunkWriter.Reset(nil)
	w.start = timeZero
	w.duration = 0
	w.seen.ClearAll()
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
	fd      fd
	flushFn flushFn
	buff    []byte
	fsync   bool
}

func newChunkWriter(flushFn flushFn, fsync bool) *chunkWriter {
	return &chunkWriter{
		flushFn: flushFn,
		buff:    make([]byte, chunkHeaderLen),
		fsync:   fsync,
	}
}

func (w *chunkWriter) Reset(f fd) {
	w.fd = f
}

func (w *chunkWriter) Close() error {
	return w.fd.Close()
}

func (w *chunkWriter) IsOpen() bool {
	return w.fd != nil
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
	if err != nil {
		w.flushFn(err)
		return n, err
	}

	// Fsync if required to
	if w.fsync {
		err = w.fd.Sync()
	}

	// Fire flush callback
	w.flushFn(err)
	return n, err
}
