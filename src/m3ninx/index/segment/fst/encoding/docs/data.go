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

package docs

import (
	"fmt"
	"io"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
)

const initialDataEncoderLen = 1024

// DataWriter writes the data file for documents.
type DataWriter struct {
	writer io.Writer
	enc    *encoding.Encoder
}

// NewDataWriter returns a new DataWriter.
func NewDataWriter(w io.Writer) *DataWriter {
	return &DataWriter{
		writer: w,
		enc:    encoding.NewEncoder(initialDataEncoderLen),
	}
}

func (w *DataWriter) Write(d doc.Document) (int, error) {
	n := w.enc.PutBytes(d.ID)
	n += w.enc.PutUvarint(uint64(len(d.Fields)))
	for _, f := range d.Fields {
		n += w.enc.PutBytes(f.Name)
		n += w.enc.PutBytes(f.Value)
	}

	if err := w.write(); err != nil {
		return 0, err
	}

	return n, nil
}

func (w *DataWriter) write() error {
	b := w.enc.Bytes()
	n, err := w.writer.Write(b)
	if err != nil {
		return err
	}
	if n < len(b) {
		return io.ErrShortWrite
	}
	w.enc.Reset()
	return nil
}

// Reset resets the DataWriter.
func (w *DataWriter) Reset(wr io.Writer) {
	w.writer = wr
	w.enc.Reset()
}

// DataReader is a reader for the data file for documents.
type DataReader struct {
	data []byte
}

// NewDataReader returns a new DataReader.
func NewDataReader(data []byte) *DataReader {
	return &DataReader{
		data: data,
	}
}

func (r *DataReader) Read(offset uint64) (doc.Document, error) {
	if offset >= uint64(len(r.data)) {
		return doc.Document{}, fmt.Errorf("invalid offset: %v is past the end of the data file", offset)
	}
	dec := encoding.NewDecoder(r.data[int(offset):])
	id, err := dec.Bytes()
	if err != nil {
		return doc.Document{}, err
	}

	x, err := dec.Uvarint()
	if err != nil {
		return doc.Document{}, err
	}
	n := int(x)

	d := doc.Document{
		ID:     id,
		Fields: make([]doc.Field, n),
	}

	for i := 0; i < n; i++ {
		name, err := dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		val, err := dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		d.Fields[i] = doc.Field{
			Name:  name,
			Value: val,
		}
	}

	return d, nil
}

// EncodedDataReader is a reader for the data file for encoded documents.
type EncodedDataReader struct {
	data []byte
}

// NewEncodedDataReader returns a new EncodedDataReader.
func NewEncodedDataReader(data []byte) *EncodedDataReader {
	return &EncodedDataReader{
		data: data,
	}
}

// Read reads a doc.EncodedDocument from a data stream starting at the specified offset.
func (e *EncodedDataReader) Read(offset uint64) (doc.EncodedDocument, error) {
	if offset >= uint64(len(e.data)) {
		return doc.EncodedDocument{}, fmt.Errorf(
			"invalid offset: %v is past the end of the data file", offset,
		)
	}

	return doc.EncodedDocument{
		Bytes: e.data[int(offset):],
	}, nil
}

// EncodedDocumentReader is a reader for reading documents from encoded documents.
type EncodedDocumentReader struct {
	currFields []doc.Field
}

// NewEncodedDocumentReader returns a new EncodedDocumentReader.
func NewEncodedDocumentReader() *EncodedDocumentReader {
	return &EncodedDocumentReader{}
}

// Read reads a doc.Document from a doc.EncodedDocument. Returned doc.Document should be
// processed before calling Read again as the underlying array pointed to by the Fields
// slice will be updated. This approach avoids allocating a new slice with a new backing
// array for every document processed, unlike (*DataReader).Read
func (r *EncodedDocumentReader) Read(encoded doc.EncodedDocument) (doc.Document, error) {
	for i := range r.currFields {
		r.currFields[i] = doc.Field{}
	}
	r.currFields = r.currFields[:0]
	id, buf, err := encoding.ReadBytes(encoded.Bytes)
	if err != nil {
		return doc.Document{}, err
	}

	x, buf, err := encoding.ReadUvarint(buf)
	if err != nil {
		return doc.Document{}, err
	}
	n := int(x)

	var name, val []byte
	for i := 0; i < n; i++ {
		name, buf, err = encoding.ReadBytes(buf)
		if err != nil {
			return doc.Document{}, err
		}
		val, buf, err = encoding.ReadBytes(buf)
		if err != nil {
			return doc.Document{}, err
		}
		r.currFields = append(r.currFields, doc.Field{
			Name:  name,
			Value: val,
		})
	}

	return doc.Document{
		ID:     id,
		Fields: r.currFields,
	}, nil
}

// ReadEncodedDocumentID reads the document ID from the encoded document.
func ReadEncodedDocumentID(encoded doc.EncodedDocument) ([]byte, error) {
	id, _, err := encoding.ReadBytes(encoded.Bytes)
	return id, err
}
