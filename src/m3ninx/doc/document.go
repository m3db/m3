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
	errReservedFieldName = fmt.Errorf("'%s' is a reserved field name", IDReservedFieldName)
	errEmptyDocument     = errors.New("document cannot be empty")
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

// Closer is a resource that can be closed.
type Closer interface {
	Close()
}

var _ Closer = CloserFn(func() {})

// CloserFn should be called when finished with a document value.
type CloserFn func()

// Close will call the closer function on close.
func (f CloserFn) Close() {
	f()
}

// NoopCloser is a no-op closer.
var NoopCloser = CloserFn(func() {})

// DecodeID is a function that can decode an encoded ID, it
// is provided when a document that has an ID that is encoded
// that can be restored with a decoder.
type DecodeID func([]byte) ([]byte, Closer, error)

// DecodeFields is a function that can decode encoded fields, it
// is provided when a document has fields that are encoded
// that can be restored with an iterator.
type DecodeFields func([]byte) ([]Field, Closer, error)

// Document represents a document to be indexed.
type Document struct {
	id     []byte
	fields []Field

	decodeID  DecodeID
	encodedID []byte

	encodedFields []byte
	decodeFields  DecodeFields
}

type DocumentBuilder struct {
	ID     []byte
	Fields []Field

	EncodedID []byte
	DecodeID  DecodeID

	EncodedFields []byte
	DecodeFields  DecodeFields
}

func (b DocumentBuilder) Build() Document {
	var doc Document
	if b.DecodeID != nil {
		doc.encodedID = b.EncodedID
		doc.decodeID = b.DecodeID
	} else {
		doc.id = b.ID
	}
	if b.DecodeFields != nil {
		doc.encodedFields = b.EncodedFields
		doc.decodeFields = b.DecodeFields
	} else {
		doc.fields = b.Fields
	}
	return doc
}

func (d Document) ID() ([]byte, Closer, error) {
	if d.decodeID != nil {
		return d.decodeID(d.encodedID)
	}
	return d.id, NoopCloser, nil
}

func (d Document) Fields() ([]Field, Closer, error) {
	if d.decodeFields != nil {
		return d.decodeFields(d.encodedFields)
	}
	return d.fields, NoopCloser, nil
}

// Get returns the value of the specified field name in the document if it exists.
func (d Document) Get(fieldName []byte) ([]byte, bool) {
	fields, closer, err := d.Fields()
	if err != nil {
		return nil, false
	}

	defer closer.Close()

	for _, f := range fields {
		if bytes.Equal(fieldName, f.Name) {
			return f.Value, true
		}
	}

	return nil, false
}

// Compare returns an integer comparing two documents. The result will be 0 if the documents
// are equal, -1 if d is ordered before other, and 1 if d is ordered aftered other.
func (d Document) Compare(other Document) (int, error) {
	id, idCloser, idErr := d.ID()
	fields, fieldsCloser, fieldsErr := d.Fields()
	otherID, otherIDCloser, otherIDErr := other.ID()
	otherFields, otherFieldsCloser, otherFieldsErr := other.Fields()
	defer func() {
		if idCloser != nil {
			idCloser.Close()
		}
		if fieldsCloser != nil {
			fieldsCloser.Close()
		}
		if otherIDCloser != nil {
			otherIDCloser.Close()
		}
		if otherFieldsCloser != nil {
			otherFieldsCloser.Close()
		}
	}()
	if idErr != nil {
		return 0, idErr
	}
	if fieldsErr != nil {
		return 0, fieldsErr
	}
	if otherIDErr != nil {
		return 0, otherIDErr
	}
	if otherFieldsErr != nil {
		return 0, otherFieldsErr
	}

	if c := bytes.Compare(id, otherID); c != 0 {
		return c, nil
	}

	l, r := Fields(fields), Fields(otherFields)

	// Make a shallow copy of the Fields so we don't mutate the document.
	if !sort.IsSorted(l) {
		l = l.shallowCopy()
		sort.Sort(l)
	}
	if !sort.IsSorted(r) {
		r = r.shallowCopy()
		sort.Sort(r)
	}

	min := len(l)
	if len(r) < min {
		min = len(r)
	}

	for i := 0; i < min; i++ {
		if c := bytes.Compare(l[i].Name, r[i].Name); c != 0 {
			return c, nil
		}
		if c := bytes.Compare(l[i].Value, r[i].Value); c != 0 {
			return c, nil
		}
	}

	if len(l) < len(r) {
		return -1, nil
	} else if len(l) > len(r) {
		return 1, nil
	}

	return 0, nil
}

// Equal returns a bool indicating whether d is equal to other.
func (d Document) Equal(other Document) (bool, error) {
	cmp, err := d.Compare(other)
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// Validate returns a bool indicating whether the document is valid.
func (d Document) Validate() error {
	id, idCloser, idErr := d.ID()
	fields, fieldsCloser, fieldsErr := d.Fields()
	defer func() {
		if idCloser != nil {
			idCloser.Close()
		}
		if fieldsCloser != nil {
			fieldsCloser.Close()
		}
	}()
	if idErr != nil {
		return idErr
	}
	if fieldsErr != nil {
		return fieldsErr
	}

	hasID, err := d.HasID()
	if err != nil {
		return err
	}

	if len(fields) == 0 && !hasID {
		return errEmptyDocument
	}

	if !utf8.Valid(id) {
		return fmt.Errorf("document contains invalid id: %v", id)
	}

	for _, f := range fields {
		// TODO: Should we enforce uniqueness of field names?
		if !utf8.Valid(f.Name) {
			return fmt.Errorf("document contains invalid field name: %v", f.Name)
		}

		if bytes.Equal(f.Name, IDReservedFieldName) {
			return errReservedFieldName
		}

		if !utf8.Valid(f.Value) {
			return fmt.Errorf("document contains invalid field value: %v", f.Value)
		}
	}

	return nil
}

// HasID returns a bool indicating whether the document has an ID or not.
func (d Document) HasID() (bool, error) {
	id, closer, err := d.ID()
	result := len(id) > 0
	closer.Close()
	if err != nil {
		return false, err
	}
	return result, nil
}

func (d Document) String() string {
	var (
		id, idCloser, idErr                  = d.ID()
		fieldsSlice, fieldsCloser, fieldsErr = d.Fields()
		fields                               string
		buf                                  bytes.Buffer
	)
	if idErr != nil {
		id = []byte(fmt.Sprintf("<err=%s>", idErr))
	}
	if fieldsErr != nil {
		fields = fmt.Sprintf("<err=%s>", fieldsErr)
	} else {
		for i, f := range fieldsSlice {
			buf.WriteString(fmt.Sprintf("%s: %s", f.Name, f.Value))
			if i != len(fieldsSlice)-1 {
				buf.WriteString(", ")
			}
		}
		fields = buf.String()
	}

	result := fmt.Sprintf("{id: %s, fields: {%s}}", id, fields)

	idCloser.Close()
	fieldsCloser.Close()

	return result
}

// SortDocuments will sort a set of documents.
func SortDocuments(docs []Document) error {
	var result error
	sort.Slice(docs, func(i, j int) bool {
		cmp, err := docs[i].Compare(docs[j])
		if err != nil && result == nil {
			result = err
		}
		return cmp < 0
	})
	return result
}
