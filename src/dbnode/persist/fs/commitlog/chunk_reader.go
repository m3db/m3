// Copyright (c) 2017 Uber Technologies, Inc.
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
	"io"
	"os"

	"github.com/m3db/m3/src/dbnode/digest"
)

const (
	sizeStart         = 0
	sizeEnd           = chunkHeaderSizeLen
	checksumSizeStart = sizeEnd
	checksumSizeEnd   = checksumSizeStart + chunkHeaderSizeLen
	checksumDataStart = checksumSizeEnd
	checksumDataEnd   = checksumDataStart + chunkHeaderChecksumDataLen
)

type chunkReader struct {
	fd                 *os.File
	buffer             *bufio.Reader
	chunkData          []byte
	chunkDataRemaining int
	charBuff           []byte
}

func newChunkReader(bufferLen int) *chunkReader {
	return &chunkReader{
		buffer:    bufio.NewReaderSize(nil, bufferLen),
		chunkData: make([]byte, bufferLen),
		charBuff:  make([]byte, 1),
	}
}

func (r *chunkReader) reset(fd *os.File) {
	r.fd = fd
	r.buffer.Reset(fd)
	r.chunkDataRemaining = 0
}

func (r *chunkReader) readHeader() error {
	header, err := r.buffer.Peek(chunkHeaderLen)
	if err != nil {
		return err
	}

	size := endianness.Uint32(header[sizeStart:sizeEnd])
	checksumSize := digest.
		Buffer(header[checksumSizeStart:checksumSizeEnd]).
		ReadDigest()
	checksumData := digest.
		Buffer(header[checksumDataStart:checksumDataEnd]).
		ReadDigest()

	// Verify size checksum
	if digest.Checksum(header[sizeStart:sizeEnd]) != checksumSize {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Discard the peeked header
	if _, err := r.buffer.Discard(chunkHeaderLen); err != nil {
		return err
	}

	// Setup a chunk data buffer so that chunk data can be loaded into it.
	chunkDataSize := int(size)
	if chunkDataSize > cap(r.chunkData) {
		// Increase chunkData capacity so that it can fit the new chunkData.
		chunkDataCap := cap(r.chunkData)
		for chunkDataCap < chunkDataSize {
			chunkDataCap *= 2
		}
		r.chunkData = make([]byte, chunkDataSize, chunkDataCap)
	} else {
		// Reuse existing chunk data buffer if possible.
		r.chunkData = r.chunkData[:chunkDataSize]
	}

	// To validate checksum of chunk data all the chunk data needs to be loaded into memory at once. Chunk data size is // not bounded to the flush size so peeking chunk data in order to compute checksum may result in bufio's buffer
	// full error. To circumnavigate this issue load the chunk data into chunk reader's buffer to compute checksum
	// instead of trying to compute checksum off of fixed size r.buffer by peeking.
	// See https://github.com/m3db/m3/pull/2148 for details.
	_, err = io.ReadFull(r.buffer, r.chunkData)
	if err != nil {
		return err
	}

	// Verify data checksum
	if digest.Checksum(r.chunkData) != checksumData {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Set remaining data to be consumed
	r.chunkDataRemaining = int(size)

	return nil
}

func (r *chunkReader) Read(p []byte) (int, error) {
	size := len(p)
	read := 0
	// Check if requesting for size larger than this chunk
	if r.chunkDataRemaining < size {
		// Copy any remaining
		if r.chunkDataRemaining > 0 {
			chunkDataOffset := len(r.chunkData) - r.chunkDataRemaining
			n := copy(p, r.chunkData[chunkDataOffset:])
			r.chunkDataRemaining -= n
			read += n
		}

		// Read next header
		if err := r.readHeader(); err != nil {
			return read, err
		}

		// Reset read target
		p = p[read:]

		// Perform consecutive read(s)
		n, err := r.Read(p)
		read += n
		return read, err
	}

	chunkDataOffset := len(r.chunkData) - r.chunkDataRemaining
	n := copy(p, r.chunkData[chunkDataOffset:][:len(p)])
	r.chunkDataRemaining -= n
	read += n
	return read, nil
}

func (r *chunkReader) ReadByte() (c byte, err error) {
	if _, err := r.Read(r.charBuff); err != nil {
		return byte(0), err
	}
	return r.charBuff[0], nil
}
