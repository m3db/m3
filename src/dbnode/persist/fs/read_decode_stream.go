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

package fs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
)

type dataFileSetReaderDecoderStream interface {
	msgpack.ByteDecoderStream

	// reader returns the underlying reader with access to the
	// incremental computed digest
	reader() digest.ReaderWithDigest
}

type readerDecoderStream struct {
	bytesReader      *bytes.Reader
	readerWithDigest digest.ReaderWithDigest
	backingBytes     []byte
	buf              [64]byte
	lastReadByte     int
	unreadByte       int
}

func newReaderDecoderStream() dataFileSetReaderDecoderStream {
	return &readerDecoderStream{
		readerWithDigest: digest.NewReaderWithDigest(nil),
		bytesReader:      bytes.NewReader(nil),
	}
}

func (s *readerDecoderStream) reader() digest.ReaderWithDigest {
	return s.readerWithDigest
}

func (s *readerDecoderStream) Reset(d []byte) {
	s.bytesReader.Reset(d)
	s.readerWithDigest.Reset(s.bytesReader)
	s.backingBytes = d
	s.lastReadByte = -1
	s.unreadByte = -1
}

func (s *readerDecoderStream) Read(p []byte) (int, error) {
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
	n, err := s.readerWithDigest.Read(p)
	n += numUnreadByte
	if n > 0 {
		s.lastReadByte = int(ref[n-1])
	}
	if err == io.EOF && n > 0 {
		return n, nil // return EOF next time, might be returning last byte still
	}
	return n, err
}

func (s *readerDecoderStream) ReadByte() (byte, error) {
	if s.unreadByte >= 0 {
		r := byte(s.unreadByte)
		s.unreadByte = -1
		return r, nil
	}
	n, err := s.readerWithDigest.Read(s.buf[:1])
	if n > 0 {
		s.lastReadByte = int(s.buf[0])
	}
	return s.buf[0], err
}

func (s *readerDecoderStream) UnreadByte() error {
	if s.lastReadByte < 0 {
		return fmt.Errorf("no previous read byte or already unread byte")
	}
	s.unreadByte = s.lastReadByte
	s.lastReadByte = -1
	return nil
}

func (s *readerDecoderStream) Bytes() []byte {
	return s.backingBytes
}

func (s *readerDecoderStream) Skip(length int64) error {
	// NB(r): This ensures the reader with digest is always read
	// from start to end, i.e. to calculate digest properly.
	remaining := length
	for {
		readEnd := int64(len(s.buf))
		if remaining < readEnd {
			readEnd = remaining
		}
		n, err := s.Read(s.buf[:readEnd])
		if err != nil {
			return err
		}
		remaining -= int64(n)
		if remaining < 0 {
			return fmt.Errorf("skipped too far, remaining is: %d", remaining)
		}
		if remaining == 0 {
			return nil
		}
	}
}

func (s *readerDecoderStream) Remaining() int64 {
	var unreadBytes int64
	if s.unreadByte != -1 {
		unreadBytes = 1
	}
	return int64(s.bytesReader.Len()) + unreadBytes
}

func (s *readerDecoderStream) Offset() int {
	return len(s.backingBytes) - int(s.Remaining())
}
