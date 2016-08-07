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
	"hash/adler32"
	"os"
	"time"

	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
)

var (
	errCommitLogReaderAlreadyOpen               = errors.New("commit log reader already open")
	errCommitLogReaderChunkSizeChecksumMismatch = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderChunkDataChecksumMismatch = errors.New("commit log reader encountered chunk data checksum mismatch")
	errCommitLogReaderMissingLogMetadata        = errors.New("commit log reader encountered message missing metadata")
)

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (time.Time, time.Duration, int, error)

	// Read returns the next key and data pair or error, will return io.EOF at end of volume
	Read() (m3db.CommitLogSeries, m3db.Datapoint, xtime.Unit, m3db.Annotation, error)

	// Close the reader
	Close() error
}

type reader struct {
	opts           m3db.DatabaseOptions
	chunkReader    *chunkReader
	sizeBuffer     []byte
	dataBuffer     []byte
	info           schema.CommitLogInfo
	log            schema.CommitLog
	metadata       schema.CommitLogMetadata
	metadataLookup map[uint64]m3db.CommitLogSeries
}

func newCommitLogReader(opts m3db.DatabaseOptions) commitLogReader {
	return &reader{
		opts:           opts,
		chunkReader:    newChunkReader(opts.GetCommitLogFlushSize()),
		sizeBuffer:     make([]byte, binary.MaxVarintLen64),
		metadataLookup: make(map[uint64]m3db.CommitLogSeries),
	}
}

func (r *reader) Open(filePath string) (time.Time, time.Duration, int, error) {
	if r.chunkReader.fd != nil {
		return timeZero, 0, 0, errCommitLogReaderAlreadyOpen
	}

	fd, err := os.Open(filePath)
	if err != nil {
		return timeZero, 0, 0, err
	}

	r.chunkReader.reset(fd)
	r.info = schema.CommitLogInfo{}
	if err := r.read(&r.info); err != nil {
		r.Close()
		return timeZero, 0, 0, err
	}

	start := time.Unix(0, r.info.Start)
	duration := time.Duration(r.info.Duration)
	index := int(r.info.Index)
	return start, duration, index, nil
}

func (r *reader) Read() (
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
	unit xtime.Unit,
	annotation m3db.Annotation,
	resultErr error,
) {
	if err := r.read(&r.log); err != nil {
		resultErr = err
		return
	}

	if len(r.log.Metadata) != 0 {
		if err := proto.Unmarshal(r.log.Metadata, &r.metadata); err != nil {
			resultErr = err
			return
		}
		r.metadataLookup[r.log.Idx] = m3db.CommitLogSeries{
			UniqueIndex: r.log.Idx,
			ID:          r.metadata.Id,
			Shard:       r.metadata.Shard,
		}
	}

	metadata, ok := r.metadataLookup[r.log.Idx]
	if !ok {
		resultErr = errCommitLogReaderMissingLogMetadata
		return
	}

	series = metadata
	datapoint = m3db.Datapoint{
		Timestamp: time.Unix(0, r.log.Timestamp),
		Value:     r.log.Value,
	}
	unit = xtime.Unit(byte(r.log.Unit))
	annotation = r.log.Annotation
	return
}

func (r *reader) read(message proto.Message) error {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return err
	}

	// Extend buffer as necessary
	if len(r.dataBuffer) < int(size) {
		diff := int(size) - len(r.dataBuffer)
		for i := 0; i < diff; i++ {
			r.dataBuffer = append(r.dataBuffer, 0)
		}
	}

	// Size the target buffer for reading and unmarshalling
	buffer := r.dataBuffer[:size]

	// Read message
	if _, err := r.chunkReader.Read(buffer); err != nil {
		return err
	}
	if err := proto.Unmarshal(buffer, message); err != nil {
		return err
	}
	return nil
}

func (r *reader) Close() error {
	if r.chunkReader.fd == nil {
		return nil
	}

	if err := r.chunkReader.fd.Close(); err != nil {
		return err
	}

	r.chunkReader.fd = nil
	r.metadataLookup = make(map[uint64]m3db.CommitLogSeries)
	return nil
}

type chunkReader struct {
	fd        *os.File
	buffer    *bufio.Reader
	remaining int
	charBuff  []byte
}

func newChunkReader(bufferLen int) *chunkReader {
	return &chunkReader{
		buffer:   bufio.NewReaderSize(nil, bufferLen),
		charBuff: make([]byte, 1),
	}
}

func (r *chunkReader) reset(fd *os.File) {
	r.fd = fd
	r.buffer.Reset(fd)
	r.remaining = 0
}

func (r *chunkReader) readHeader() error {
	header, err := r.buffer.Peek(chunkHeaderLen)
	if err != nil {
		return err
	}

	sizeStart, sizeEnd := 0, chunkHeaderSizeLen
	checksumSizeStart, checksumSizeEnd := sizeEnd, sizeEnd+chunkHeaderSizeLen
	checksumDataStart, checksumDataEnd := checksumSizeEnd, checksumSizeEnd+chunkHeaderChecksumDataLen
	size := endianness.Uint32(header[sizeStart:sizeEnd])
	checksumSize := endianness.Uint32(header[checksumSizeStart:checksumSizeEnd])
	checksumData := endianness.Uint32(header[checksumDataStart:checksumDataEnd])

	// Verify size checksum
	if adler32.Checksum(header[:4]) != checksumSize {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Discard the peeked header
	if _, err := r.buffer.Discard(chunkHeaderLen); err != nil {
		return err
	}

	// Verify data checksum
	data, err := r.buffer.Peek(int(size))
	if err != nil {
		return err
	}

	if adler32.Checksum(data) != checksumData {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Set remaining data to be consumed
	r.remaining = int(size)

	return nil
}

func (r *chunkReader) Read(p []byte) (int, error) {
	size := len(p)
	read := 0
	// Check if requesting for size larger than this chunk
	if r.remaining < size {
		// Copy any remaining
		if r.remaining > 0 {
			n, err := r.buffer.Read(p[:r.remaining])
			r.remaining -= n
			read += n
			if err != nil {
				return read, err
			}
		}

		// Read next header
		if err := r.readHeader(); err != nil {
			return read, err
		}

		// Reset read target
		p = p[read:]
		size = len(p)

		// Perform consecutive read(s)
		n, err := r.Read(p)
		read += n
		return read, err
	}

	n, err := r.buffer.Read(p)
	r.remaining -= n
	read += n
	return read, err
}

func (r *chunkReader) ReadByte() (c byte, err error) {
	if _, err := r.Read(r.charBuff); err != nil {
		return byte(0), err
	}
	return r.charBuff[0], nil
}
