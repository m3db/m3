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

package segment

import (
	"github.com/m3db/m3ninx/doc"
)

// ReservedFieldName signifies the field names which are reserved for internal
// use.
type ReservedFieldName string

const (
	// IDFieldName is the fieldname used for the `doc/Document.ID` field.
	IDFieldName ReservedFieldName = "_id"
)

// ID is a unique id per segment.
type ID int64

// Segment is a collection of documents indexed by the fields
// identified by `Document.IndexableFields`.
type Segment interface {
	// ID returns the Segment's unique identifier.
	ID() ID

	// Size returns the number of documents indexed.
	Size() uint32

	// Iter returns a segment document iterator.
	Iter() Iter
}

// Iter allows for iteration over the documents indexed in the segment.
type Iter interface {
	// Next returns a bool indicating if the iterator has any more documents
	// to return.
	Next() bool

	// Current returns the current document and the DocID associated with the
	// document in the segment.
	Current() (doc.Document, DocID)

	// Err returns any errors the Iter ran into whilst iterating.
	Err() error
}

// Readable is a readable index segment.
type Readable interface {
	// Query retrieves the list of documents matching the given criterion.
	Query(q Query) []doc.Document
}

// Writable is a writable index segment.
type Writable interface {
	// Insert inserts the given documents with provided fields.
	Insert(d doc.Document) error

	// Update updates the given document.
	Update(d doc.Document) error

	// Delete deletes the given document.
	Delete(d doc.Document) error
}

// Writer represents a segment writer.
type Writer interface {
	Writable

	// Open prepares the writer to accept writes.
	Open() error

	// Close closes the writer.
	Close() error
}

// Reader represents a segment reader.
type Reader interface {
	Readable

	// Open prepares the reader to accept reads.
	Open() error

	// Close closes the reader.
	Close() error
}

// DocID is document identifier internal to each index segment. It's limited to
// be at most 2^32.
type DocID uint32

// QueryConjunction specifies query operation conjunction.
type QueryConjunction byte

const (
	// UnknownConjunction is the default unknown conjunction.
	UnknownConjunction QueryConjunction = iota
	// AndConjunction specifies logically 'And' all provided criterion.
	AndConjunction
)

// Query is a group of criterion to filter documents by.
type Query struct {
	Conjunction QueryConjunction
	Filters     []Filter
	SubQueries  []Query
}

// Filter is a field value filter.
type Filter struct {
	// FiledName is the name of an index field associated with a document.
	FieldName []byte
	// FieldValueFilter is the filter applied to field values for the given name.
	FieldValueFilter []byte
	// Negate specifies if matches should be negated.
	Negate bool
	// Regexp specifies whether the FieldValueFilter should be treated as a Regexp.
	// Note: RE2 only, no PCRE (i.e. no backreferences).
	Regexp bool
}
