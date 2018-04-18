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

package doc

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"
)

var (
	errMultipleIDs   = errors.New("document cannot contain multiple IDs")
	errZeroLengthID  = errors.New("document ID cannot be of length zero")
	errEmptyDocument = errors.New("document cannot be empty")
)

// IDReservedFieldName is the field name reserved for IDs.
var IDReservedFieldName = []byte("_m3ninx_id")

// Field represents a field in a document. It is composed of a name and a value.
type Field struct {
	Name  []byte
	Value []byte
}

// Fields is a list of fields.
type Fields []Field

func (f Fields) Len() int {
	return len(f)
}

func (f Fields) Less(i, j int) bool {
	l, r := f[i], f[j]

	c := bytes.Compare(l.Name, r.Name)
	switch {
	case c < 0:
		return true
	case c > 0:
		return false
	}

	c = bytes.Compare(l.Value, r.Value)
	switch {
	case c < 0:
		return true
	case c > 0:
		return false
	}

	return true
}

func (f Fields) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f Fields) shallowCopy() Fields {
	cp := make([]Field, 0, len(f))
	for _, fld := range f {
		cp = append(cp, Field{
			Name:  fld.Name,
			Value: fld.Value,
		})
	}
	return cp
}

// Document represents a document to be indexed.
type Document struct {
	// Fields contains the list of fields by which to index the document.
	Fields []Field
}

// Get returns the value of the specified field name in the document if it exists.
func (d Document) Get(fieldName []byte) ([]byte, bool) {
	for _, f := range d.Fields {
		if bytes.Equal(fieldName, f.Name) {
			return f.Value, true
		}
	}
	return nil, false
}

// Equal returns whether this document is equal to another.
func (d Document) Equal(other Document) bool {
	if len(d.Fields) != len(other.Fields) {
		return false
	}

	l, r := Fields(d.Fields), Fields(other.Fields)

	// Make a shallow copy of the Fields so we don't mutate the document.
	if !sort.IsSorted(l) {
		l = l.shallowCopy()
		sort.Sort(l)
	}
	if !sort.IsSorted(r) {
		r = r.shallowCopy()
		sort.Sort(r)
	}

	for i := range l {
		if !bytes.Equal(l[i].Name, r[i].Name) {
			return false
		}
		if !bytes.Equal(l[i].Value, r[i].Value) {
			return false
		}
	}

	return true
}

// Validate validates the given document and returns its ID if it has one.
func (d Document) Validate() ([]byte, error) {
	if len(d.Fields) == 0 {
		return nil, errEmptyDocument
	}

	var id []byte
	for _, f := range d.Fields {
		// TODO: Should we enforce uniqueness of field names?
		if !utf8.Valid(f.Name) {
			return nil, fmt.Errorf("document contains invalid field name: %v", f.Name)
		}

		if bytes.Equal(f.Name, IDReservedFieldName) {
			if id != nil {
				return nil, errMultipleIDs
			}

			if len(f.Value) == 0 {
				return nil, errZeroLengthID
			}

			id = f.Value
		}

		if !utf8.Valid(f.Value) {
			return nil, fmt.Errorf("document contains invalid field value: %v", f.Value)
		}
	}

	return id, nil
}
