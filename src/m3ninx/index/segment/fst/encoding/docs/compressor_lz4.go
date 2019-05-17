// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Softwarw.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package docs

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/pierrec/lz4"
)

func newLZ4BytesCompressor(initialSize int) (Compressor, error) {
	return &lz4BytesCompressor{
		buffer: bytes.NewBuffer(make([]byte, initialSize)),
		writer: lz4.NewWriter(nil),
	}, nil
}

type lz4BytesCompressor struct {
	buffer *bytes.Buffer
	writer *lz4.Writer
}

func (l *lz4BytesCompressor) Reset() {
	l.buffer.Reset()
	l.writer.Reset(ioutil.Discard)
}

func (l *lz4BytesCompressor) Compress(b []byte) ([]byte, error) {
	l.Reset()
	l.writer.Reset(l.buffer)
	l.writer.BlockMaxSize = 1 << 16 // 64KB
	if _, err := l.writer.Write(b); err != nil {
		return nil, err
	}
	if err := l.writer.Close(); err != nil {
		return nil, err
	}
	return l.buffer.Bytes(), nil
}

func newLZ4BytesDecompressor(copts CompressionOptions) Decompressor {
	br := bytes.NewReader(nil)
	return &lz4BytesDecompressor{
		copts:       copts,
		buffer:      bytes.NewBuffer(make([]byte, copts.PageSize)),
		bytesReader: br,
		lz4Reader:   lz4.NewReader(br),
	}
}

type lz4BytesDecompressor struct {
	copts       CompressionOptions
	buffer      *bytes.Buffer
	bytesReader *bytes.Reader
	lz4Reader   *lz4.Reader
}

func (l *lz4BytesDecompressor) Reset() {
	l.buffer.Reset()
	l.bytesReader.Reset(nil)
	l.lz4Reader.Reset(nil)
}

func (l *lz4BytesDecompressor) CompressionOptions() CompressionOptions {
	return l.copts
}

func (l *lz4BytesDecompressor) Decompress(b []byte) ([]byte, error) {
	l.buffer.Reset()
	l.bytesReader.Reset(b)
	l.lz4Reader.Reset(l.bytesReader)
	_, err := io.Copy(l.buffer, l.lz4Reader)
	if err != nil {
		return nil, err
	}
	return l.buffer.Bytes(), nil
}
