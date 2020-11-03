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
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package pilosa

import (
	"bytes"
	"errors"

	"github.com/m3db/m3/src/m3ninx/postings"
	idxroaring "github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3dbx/pilosa/roaring"
)

var errNotPilosaRoaringBitmap = errors.New("not pilosa roaring bitmap")

// Encoder helps serialize a Pilosa RoaringBitmap
type Encoder struct {
	scratchBuffer bytes.Buffer
}

// NewEncoder returns a new Encoder.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Reset resets the internal state of the encoder to allow
// for re-use.
func (e *Encoder) Reset() {
	e.scratchBuffer.Reset()
}

// Encode encodes the provided postings list in serialized form.
// The bytes returned are invalidate on a subsequent call to Encode(),
// or Reset().
func (e *Encoder) Encode(pl postings.List) ([]byte, error) {
	e.scratchBuffer.Reset()

	// Only work with pilosa roaring bitmaps since any other format
	// will cause large allocations to re-encode as a pilosa postings list
	// before writing it out.
	bitmap, ok := idxroaring.BitmapFromPostingsList(pl)
	if !ok {
		return nil, errNotPilosaRoaringBitmap
	}

	if _, err := bitmap.WriteTo(&e.scratchBuffer); err != nil {
		return nil, err
	}

	return e.scratchBuffer.Bytes(), nil
}

// Unmarshal unmarshals the provided bytes into a postings.List.
func Unmarshal(data []byte) (postings.List, error) {
	bitmap := roaring.NewBitmap()
	err := bitmap.UnmarshalBinary(data)
	if err != nil {
		return nil, err
	}
	return idxroaring.NewPostingsListFromBitmap(bitmap), nil
}
