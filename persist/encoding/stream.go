// Copyright (c) 2017 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package encoding

import (
	"bytes"
	"fmt"
	"io"
)

// DecoderStream is a data stream that is read by the decoder,
// it takes both a reader and the underlying backing bytes.
// This is constructed this way since the decoder needs access
// to the backing bytes when taking refs directly for decoding byte
// slices without allocating bytes itself but also needs to progress
// the reader (for instance when a reader is a ReaderWithDigest that
// is calculating a digest as its being read).
type DecoderStream interface {
	io.Reader

	// Reset resets the decoder stream for decoding a new byte slice.
	Reset(b []byte)

	// ReadByte reads the next byte.
	ReadByte() (byte, error)

	// UnreadByte unreads the last read byte or returns error if none read
	// yet. Only a single byte can be unread at a time, a consecutive call
	// to UnreadByte will result in an error.
	UnreadByte() error

	// Bytes returns the ref to the bytes provided when Reset(...) is
	// called. To get the current position into the byte slice use:
	// len(s.Bytes()) - s.Remaining()
	Bytes() []byte

	// Skip progresses the reader by a certain amount of bytes, useful
	// when taking a ref to some of the bytes and progressing the reader
	// itself.
	Skip(length int64) error

	// Remaining returns the remaining bytes in the stream.
	Remaining() int64
}

type decoderStream struct {
	reader       *bytes.Reader
	bytes        []byte
	lastReadByte int
	unreadByte   int
}

// NewDecoderStream creates a new decoder stream from a bytes ref.
func NewDecoderStream(b []byte) DecoderStream {
	return &decoderStream{
		reader:       bytes.NewReader(b),
		bytes:        b,
		lastReadByte: -1,
		unreadByte:   -1,
	}
}

func (s *decoderStream) Reset(b []byte) {
	s.reader.Reset(b)
	s.bytes = b
	s.lastReadByte = -1
	s.unreadByte = -1
}

func (s *decoderStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	ref := p

	var numUnreadByte int
	if s.unreadByte >= 0 {
		p[0] = byte(s.unreadByte)
		p = p[1:]
		s.unreadByte = -1
		numUnreadByte = 1
	}
	n, err := s.reader.Read(p)
	n += numUnreadByte
	if n > 0 {
		s.lastReadByte = int(ref[n-1])
	}
	if err == io.EOF && n > 0 {
		return n, nil // return EOF next time, might be returning last byte still
	}
	return n, err
}

func (s *decoderStream) ReadByte() (byte, error) {
	if s.unreadByte >= 0 {
		r := byte(s.unreadByte)
		s.unreadByte = -1
		return r, nil
	}
	b, err := s.reader.ReadByte()
	if err == nil {
		s.lastReadByte = int(b)
	}
	return b, err
}

func (s *decoderStream) UnreadByte() error {
	if s.lastReadByte < 0 {
		return fmt.Errorf("no previous read byte or already unread byte")
	}
	s.unreadByte = s.lastReadByte
	s.lastReadByte = -1
	return nil
}

func (s *decoderStream) Bytes() []byte {
	return s.bytes
}

func (s *decoderStream) Skip(length int64) error {
	defer func() {
		if length > 0 {
			s.unreadByte = -1
			s.lastReadByte = -1
		}
	}()
	_, err := s.reader.Seek(length, io.SeekCurrent)
	return err
}

func (s *decoderStream) Remaining() int64 {
	var unreadBytes int64
	if s.unreadByte != -1 {
		unreadBytes = 1
	}
	return int64(s.reader.Len()) + unreadBytes
}
