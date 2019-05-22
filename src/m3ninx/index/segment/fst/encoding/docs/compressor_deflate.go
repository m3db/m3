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
	"compress/flate"
	"errors"
	"io"
	"io/ioutil"
)

var (
	errUnableToCastFlateResetter = errors.New("unable to cast flate.Reader -> flate.Resetter")
)

func newDeflateBytesCompressor(initialSize int) (Compressor, error) {
	w, err := flate.NewWriter(nil, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	return &deflateBytesCompressor{
		buffer: bytes.NewBuffer(make([]byte, initialSize)),
		writer: w,
	}, nil
}

type deflateBytesCompressor struct {
	buffer *bytes.Buffer
	writer *flate.Writer
}

func (l *deflateBytesCompressor) Reset() {
	l.buffer.Reset()
	l.writer.Reset(ioutil.Discard)
}

func (l *deflateBytesCompressor) Compress(b []byte) ([]byte, error) {
	l.Reset()
	l.writer.Reset(l.buffer)
	if _, err := l.writer.Write(b); err != nil {
		return nil, err
	}
	if err := l.writer.Close(); err != nil {
		return nil, err
	}
	return l.buffer.Bytes(), nil
}

func newDeflateBytesDecompressor(copts CompressionOptions) Decompressor {
	br := bytes.NewReader(nil)
	return &deflateBytesDecompressor{
		copts:       copts,
		buffer:      bytes.NewBuffer(make([]byte, copts.PageSize)),
		bytesReader: br,
		flateReader: flate.NewReader(br),
	}
}

type deflateBytesDecompressor struct {
	copts       CompressionOptions
	buffer      *bytes.Buffer
	bytesReader *bytes.Reader
	flateReader io.ReadCloser
}

func (l *deflateBytesDecompressor) Reset() {
	l.buffer.Reset()
	l.bytesReader.Reset(nil)
}

func (l *deflateBytesDecompressor) CompressionOptions() CompressionOptions {
	return l.copts
}

func (l *deflateBytesDecompressor) Decompress(b []byte) ([]byte, error) {
	l.buffer.Reset()
	l.bytesReader.Reset(b)
	resetter, ok := l.flateReader.(flate.Resetter)
	if !ok {
		return nil, errUnableToCastFlateResetter
	}
	if err := resetter.Reset(l.bytesReader, nil); err != nil {
		return nil, err
	}
	_, err := io.Copy(l.buffer, l.flateReader)
	if err != nil {
		return nil, err
	}
	return l.buffer.Bytes(), nil
}
