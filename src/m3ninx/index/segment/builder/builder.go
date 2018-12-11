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

package builder

import (
	"errors"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/util"
)

var (
	errDocNotFound   = errors.New("doc not found")
	errFieldNotFound = errors.New("field not found")
	errTermNotFound  = errors.New("term not found")
)

type builder struct {
	opts      Options
	newUUIDFn util.NewUUIDFn

	offset postings.ID

	docs         []doc.Document
	idSet        *IDsMap
	fields       *fieldsMap
	uniqueFields [][]byte
}

// NewBuilder returns a segment builder, it is not thread safe
// and is optimized for insertion speed and a final build step
// when documents are indexed.
func NewBuilder(opts Options) (segment.Builder, error) {
	return &builder{
		opts:      opts,
		newUUIDFn: opts.NewUUIDFn(),
		idSet: NewIDsMap(IDsMapOptions{
			InitialSize: opts.InitialCapacity(),
		}),
		fields: newFieldsMap(fieldsMapOptions{
			InitialSize: opts.InitialCapacity(),
		}),
		uniqueFields: make([][]byte, 0, opts.InitialCapacity()),
	}, nil
}

func (b *builder) Reset(offset postings.ID) {
	b.offset = offset

	// Reset the documents slice
	var empty doc.Document
	for i := range b.docs {
		b.docs[i] = empty
	}
	b.docs = b.docs[:0]

	// Remove all entries in the ID set
	b.idSet.Reset()

	// Keep fields around, just reset the terms set for each one
	for _, entry := range b.fields.Iter() {
		entry.Value().reset()
	}

	// Reset the unique fields slice
	for i := range b.uniqueFields {
		b.uniqueFields[i] = nil
	}
	b.uniqueFields = b.uniqueFields[:0]
}

func (b *builder) Insert(d doc.Document) ([]byte, error) {
	err := b.InsertBatch(index.Batch{Docs: []doc.Document{d}})
	if err != nil {
		return nil, err
	}
	last := b.docs[len(b.docs)-1]
	return last.ID, nil
}

func (b *builder) InsertBatch(batch index.Batch) error {
	// NB(r): This is all kept in a single method to make the
	// insertion path fast.
	batchErr := index.NewBatchPartialError()
	for i, d := range batch.Docs {
		// Validate doc
		if err := d.Validate(); err != nil {
			if !batch.AllowPartialUpdates {
				return err
			}
			batchErr.Add(index.BatchError{Err: err, Idx: i})
			continue
		}

		// Generate ID if needed
		if !d.HasID() {
			id, err := b.newUUIDFn()
			if err != nil {
				if !batch.AllowPartialUpdates {
					return err
				}
				batchErr.Add(index.BatchError{Err: err, Idx: i})
				continue
			}

			d.ID = id

			// Update the document in the batch since we added an ID to it.
			batch.Docs[i] = d
		}

		// Avoid duplicates
		if _, ok := b.idSet.Get(d.ID); ok {
			if !batch.AllowPartialUpdates {
				return index.ErrDuplicateID
			}
			batchErr.Add(index.BatchError{Err: index.ErrDuplicateID, Idx: i})
			continue
		}

		// Write to document set
		b.idSet.SetUnsafe(d.ID, struct{}{}, IDsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})

		idx := len(b.docs)
		b.docs = append(b.docs, d)

		// Index the terms
		for _, f := range d.Fields {
			b.index(postings.ID(idx), f)
		}
		b.index(postings.ID(idx), doc.Field{
			Name:  doc.IDReservedFieldName,
			Value: d.ID,
		})
	}

	if batchErr.IsEmpty() {
		return nil
	}
	return batchErr
}

func (b *builder) index(id postings.ID, f doc.Field) {
	terms, ok := b.fields.Get(f.Name)
	if !ok {
		terms = newTerms(b.opts)
		b.fields.SetUnsafe(f.Name, terms, fieldsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
	}

	// If empty field, track insertion of this key into the fields
	// collection for correct response when retrieving all fields
	if terms.size() == 0 {
		b.uniqueFields = append(b.uniqueFields, f.Name)
	}

	terms.post(f.Value, id)
}

func (b *builder) AllDocs() (index.IDDocIterator, error) {
	rangeIter := postings.NewRangeIterator(b.offset,
		b.offset+postings.ID(len(b.docs)))
	return index.NewIDDocIterator(b, rangeIter), nil
}

func (b *builder) Doc(id postings.ID) (doc.Document, error) {
	idx := int(id - b.offset)
	if idx < 0 || idx >= len(b.docs) {
		return doc.Document{}, errDocNotFound
	}

	return b.docs[idx], nil
}

func (b *builder) Docs() []doc.Document {
	return b.docs
}

func (b *builder) FieldsIterable() segment.FieldsIterable {
	return b
}

func (b *builder) TermsIterable() segment.TermsIterable {
	return b
}

func (b *builder) Fields() (segment.FieldsIterator, error) {
	return NewOrderedBytesSliceIter(b.uniqueFields), nil
}

func (b *builder) Terms(field []byte) (segment.TermsIterator, error) {
	terms, ok := b.fields.Get(field)
	if !ok || terms.size() == 0 {
		return nil, errFieldNotFound
	}
	return newTermsIter(terms.uniqueTerms), nil
}
