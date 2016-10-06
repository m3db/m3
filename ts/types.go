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

package ts

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"time"
)

// ID represents an immutable identifier for a timeseries
type ID interface {
	fmt.Stringer

	Data() []byte
	Equal(value ID) bool
	Hash() Hash
}

// Hash represents a form of ID suitable to be used as map keys
type Hash [md5.Size]byte

type id struct {
	data []byte
	hash Hash
}

// Data returns the binary representation of an ID
func (id *id) Data() []byte {
	return id.data
}

func (id *id) Equal(value ID) bool {
	return bytes.Equal(id.Data(), value.Data())
}

var null = Hash{}

// Hash returns (and calculates, if needed) the hash for an ID
func (id *id) Hash() Hash {
	if bytes.Equal(id.hash[:], null[:]) {
		id.hash = md5.Sum(id.data)
	}

	return Hash(id.hash)
}

func (id *id) String() string {
	return string(id.data)
}

// BinaryID constructs a new ID based on a binary blob
func BinaryID(v []byte) ID {
	return &id{data: v}
}

// StringID constructs a new ID based on a string
func StringID(v string) ID {
	return BinaryID([]byte(v))
}

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Annotation represents information used to annotate datapoints
type Annotation []byte
