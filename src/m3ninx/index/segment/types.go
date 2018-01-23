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

// ID is a unique id per segment.
type ID int64

// Segment is a collection of documents indexed by the fields
// identified by `Document.IndexableFields`.
type Segment interface {
	// ID returns the Segment's unique identifier.
	ID() ID

	// Size returns the number of documents indexed.
	Size() uint32
}

// Readable is a readable index segment.
type Readable interface {
	// Query retrieves the list of documents matching the given criterion.
	Query(q Query) (ResultsIter, error)
}

// ResultsIter is an iterator over query results.
type ResultsIter interface {
	// Next returns a bool indicating if the iterator has any more documents
	// to return.
	Next() bool

	// Current returns the current document and whether the document was marked
	// as deleted or not.
	Current() (d doc.Document, tombstoned bool)

	// Err returns any errors encountered during iteration.
	Err() error
}

// Writable is a writable index segment.
type Writable interface {
	// Insert inserts the given documents with provided fields.
	Insert(d doc.Document) error

	// Delete deletes the given document.
	Delete(d doc.Document) error
}

// DocID is a document identifier internal to each index segment. It's limited to
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

// ImmutablePostingsList is a memory efficient collection of docIDs. It only permits
// for read-only operations.
type ImmutablePostingsList interface {
	// Contains returns whether the specified id is contained in the set.
	Contains(i DocID) bool

	// IsEmpty returns whether the set is empty.
	IsEmpty() bool

	// Size returns the numbers of ids in the set.
	Size() uint64

	// Iter returns an iterator over the known values.
	Iter() PostingsIter

	// Clone returns a copy of the set.
	Clone() PostingsList
}

// PostingsList is a memory efficient collection of docIDs.
type PostingsList interface {
	ImmutablePostingsList

	// Insert inserts the given id to set.
	Insert(i DocID) error

	// Intersect modifies the receiver set to contain only those ids by both sets.
	Intersect(other ImmutablePostingsList) error

	// Difference modifies the receiver set to remove any ids contained by both sets.
	Difference(other ImmutablePostingsList) error

	// Union modifies the receiver set to contain ids containted in either of the original sets.
	Union(other ImmutablePostingsList) error

	// Reset resets the internal state of the PostingsList.
	Reset()
}

// PostingsIter is an iterator over a set of docIDs.
type PostingsIter interface {
	// Current returns the current DocID.
	Current() DocID

	// Next returns whether we have another DocID.
	Next() bool
}

// PostingsListPool provides a pool for PostingsList(s).
type PostingsListPool interface {
	// Get retrieves a PostingsList.
	Get() PostingsList

	// Put releases the provided PostingsList back to the pool.
	Put(pl PostingsList)
}
