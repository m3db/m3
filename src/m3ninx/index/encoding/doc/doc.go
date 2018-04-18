// Copyright (c) 2018 Uber Technologies, Inc.
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

package doc

import (
	"errors"
	"fmt"
	"io"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/encoding"
)

// Documents file format metadata.
const (
	magicNumber = 0x6D33D0C5

	fileFormatVersion = 1

	headerSize = 0 +
		4 + // magic number
		4 + // version
		0

	trailerSize = 0 +
		8 + // number of documents
		4 + // checksum of payload
		0
)

const initialEncoderSize = 1024

var (
	errInvalidMagicNumber = errors.New("encountered invalid magic number")
	errChecksumMismatch   = errors.New("encountered invalid checksum")
)

type writer struct {
	version uint32

	writer io.Writer
	enc    *encoding.Encoder

	docs     uint64
	checksum *encoding.Checksum
}

// NewWriter returns a new Writer for serializing documents to an underlying io.Writer.
func NewWriter(w io.Writer) Writer {
	return &writer{
		version:  fileFormatVersion,
		writer:   w,
		enc:      encoding.NewEncoder(initialEncoderSize),
		checksum: encoding.NewChecksum(),
	}
}

func (w *writer) Open() error {
	w.enc.PutUint32(magicNumber)
	w.enc.PutUint32(w.version)
	return w.write()
}

func (w *writer) Write(d doc.Document) error {
	w.enc.PutUvarint(uint64(len(d.Fields)))

	for _, f := range d.Fields {
		w.enc.PutBytes(f.Name)
		w.enc.PutBytes(f.Value)
	}

	if err := w.write(); err != nil {
		return err
	}

	w.docs++
	return nil
}

func (w *writer) Close() error {
	w.enc.PutUint64(w.docs)
	if err := w.write(); err != nil {
		return err
	}

	w.enc.PutUint32(w.checksum.Get())
	return w.write()
}

func (w *writer) write() error {
	b := w.enc.Bytes()
	n, err := w.writer.Write(b)
	if err != nil {
		return err
	}
	if n < len(b) {
		return io.ErrShortWrite
	}
	w.checksum.Update(b)
	w.enc.Reset()
	return nil
}

type reader struct {
	version uint32

	data []byte
	dec  *encoding.Decoder

	curr     uint64
	total    uint64
	checksum *encoding.Checksum
}

// NewReader returns a new Reader for deserializing documents from a byte slice.
func NewReader(data []byte) Reader {
	return &reader{
		data:     data,
		checksum: encoding.NewChecksum(),
	}
}

func (r *reader) Open() error {
	if len(r.data) < headerSize+trailerSize {
		return io.ErrShortBuffer
	}

	if err := r.verifyChecksum(); err != nil {
		return err
	}
	if err := r.decodeHeader(); err != nil {
		return err
	}
	if err := r.decodeTrailer(); err != nil {
		return err
	}

	start := headerSize
	end := len(r.data) - trailerSize
	payload := r.data[start:end]
	r.dec = encoding.NewDecoder(payload)
	return nil
}

func (r *reader) decodeHeader() error {
	var (
		header = r.data[:headerSize]
		dec    = encoding.NewDecoder(header)
	)

	// Verify magic number.
	n, err := dec.Uint32()
	if err != nil {
		return err
	}
	if n != magicNumber {
		return errInvalidMagicNumber
	}

	// Verify file format version.
	v, err := dec.Uint32()
	if err != nil {
		return err
	}
	if v != fileFormatVersion {
		return fmt.Errorf("version of file format: %v does not match expected version: %v", v, fileFormatVersion)
	}
	r.version = v
	return nil
}

func (r *reader) decodeTrailer() error {
	var (
		trailer = r.data[len(r.data)-trailerSize:]
		dec     = encoding.NewDecoder(trailer)
	)

	// Get number of documents.
	n, err := dec.Uint64()
	if err != nil {
		return err
	}
	r.total = n
	return nil
}

func (r *reader) verifyChecksum() error {
	var (
		data     = r.data[:len(r.data)-4]
		checksum = r.data[len(r.data)-4:]
		dec      = encoding.NewDecoder(checksum)
	)

	expected, err := dec.Uint32()
	if err != nil {
		return err
	}

	r.checksum.Update(data)

	if expected != r.checksum.Get() {
		return errChecksumMismatch
	}

	return nil
}

func (r *reader) Read() (doc.Document, error) {
	if r.curr == r.total {
		return doc.Document{}, io.EOF
	}

	x, err := r.dec.Uvarint()
	if err != nil {
		return doc.Document{}, err
	}
	n := int(x)

	d := doc.Document{
		Fields: make([]doc.Field, n),
	}

	for i := 0; i < n; i++ {
		name, err := r.dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		val, err := r.dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		d.Fields[i] = doc.Field{
			Name:  name,
			Value: val,
		}
	}

	r.curr++
	return d, nil
}

func (r *reader) Close() error {
	return nil
}
