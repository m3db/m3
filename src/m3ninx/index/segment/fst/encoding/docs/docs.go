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
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
)

// Encoder encodes doc.Document.
type Encoder struct {
	enc *encoding.Encoder
}

// NewEncoder returns a new Encoder.
func NewEncoder(initialSize int) *Encoder {
	return &Encoder{
		enc: encoding.NewEncoder(initialDataEncoderLen),
	}
}

// Encode encodes the provided document.
func (e *Encoder) Encode(d doc.Document) ([]byte, error) {
	e.enc.Reset()
	n := e.enc.PutBytes(d.ID)
	n += e.enc.PutUvarint(uint64(len(d.Fields)))
	for _, f := range d.Fields {
		n += e.enc.PutBytes(f.Name)
		n += e.enc.PutBytes(f.Value)
	}
	return e.enc.Bytes(), nil
}

// Reset resets the encoder.
func (e *Encoder) Reset() {
	e.enc.Reset()
}

// Decoder decodes doc.Document.
type Decoder struct {
	dec *encoding.Decoder
}

// NewDecoder returns a new Decoder.
func NewDecoder() *Decoder {
	return &Decoder{
		dec: encoding.NewDecoder(nil),
	}
}

// Decode decodes the provided document.
func (d *Decoder) Decode(b []byte) (doc.Document, error) {
	d.dec.Reset(b)
	id, err := d.dec.Bytes()
	if err != nil {
		return doc.Document{}, err
	}

	x, err := d.dec.Uvarint()
	if err != nil {
		return doc.Document{}, err
	}
	n := int(x)

	dd := doc.Document{
		ID:     id,
		Fields: make([]doc.Field, n),
	}

	for i := 0; i < n; i++ {
		name, err := d.dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		val, err := d.dec.Bytes()
		if err != nil {
			return doc.Document{}, err
		}
		dd.Fields[i] = doc.Field{
			Name:  name,
			Value: val,
		}
	}

	return dd, nil
}
