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

package convert

import (
	"bytes"
	"errors"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	// ReservedFieldNameID is the field name used to index the ID in the
	// m3ninx subsytem.
	ReservedFieldNameID = doc.IDReservedFieldName

	// ErrUsingReservedFieldName is the error returned when a metric
	// cannot be parsed due to using a resereved field name
	ErrUsingReservedFieldName = errors.New(
		"unable to parse metric using reserved field name: " +
			string(ReservedFieldNameID))

	errInvalidResultMissingID = errors.New(
		"corrupt data, unable to extract id")
)

// ValidateMetric will validate a metric for use in the m3ninx subsytem
// FOLLOWUP(r): Rename ValidateMetric to ValidateSeries (metric terminiology
// is not common in the codebase)
func ValidateMetric(id ident.ID, tags ident.Tags) error {
	for _, tag := range tags.Values() {
		if bytes.Equal(ReservedFieldNameID, tag.Name.Bytes()) {
			return ErrUsingReservedFieldName
		}
	}
	return nil
}

// FromMetric converts the provided metric id+tags into a document.
// FOLLOWUP(r): Rename FromMetric to FromSeries (metric terminiology
// is not common in the codebase)
func FromMetric(id ident.ID, tags ident.Tags) (doc.Document, error) {
	clonedID := clone(id)
	fields := make([]doc.Field, 0, len(tags.Values()))
	for _, tag := range tags.Values() {
		if bytes.Equal(ReservedFieldNameID, tag.Name.Bytes()) {
			return doc.Document{}, ErrUsingReservedFieldName
		}

		nameBytes, valueBytes := tag.Name.Bytes(), tag.Value.Bytes()

		var clonedName, clonedValue []byte
		if idx := bytes.Index(clonedID, nameBytes); idx != -1 {
			clonedName = clonedID[idx : idx+len(nameBytes)]
		} else {
			clonedName = append([]byte(nil), nameBytes...)
		}
		if idx := bytes.Index(clonedID, valueBytes); idx != -1 {
			clonedValue = clonedID[idx : idx+len(valueBytes)]
		} else {
			clonedValue = append([]byte(nil), valueBytes...)
		}

		fields = append(fields, doc.Field{
			Name:  clonedName,
			Value: clonedValue,
		})
	}
	return doc.Document{
		ID:     clonedID,
		Fields: fields,
	}, nil
}

// FromMetricNoClone converts the provided metric id+tags into a document without cloning.
func FromMetricNoClone(id ident.ID, tags ident.Tags) (doc.Document, error) {
	fields := make([]doc.Field, 0, len(tags.Values()))
	for _, tag := range tags.Values() {
		if bytes.Equal(ReservedFieldNameID, tag.Name.Bytes()) {
			return doc.Document{}, ErrUsingReservedFieldName
		}
		fields = append(fields, doc.Field{
			Name:  tag.Name.Bytes(),
			Value: tag.Value.Bytes(),
		})
	}
	return doc.Document{
		ID:     id.Bytes(),
		Fields: fields,
	}, nil
}

// FromMetricIter converts the provided metric id+tags into a document.
// FOLLOWUP(r): Rename FromMetric to FromSeries (metric terminiology
// is not common in the codebase)
func FromMetricIter(id ident.ID, tags ident.TagIterator) (doc.Document, error) {
	clonedID := clone(id)
	fields := make([]doc.Field, 0, tags.Remaining())
	for tags.Next() {
		tag := tags.Current()
		if bytes.Equal(ReservedFieldNameID, tag.Name.Bytes()) {
			return doc.Document{}, ErrUsingReservedFieldName
		}

		nameBytes, valueBytes := tag.Name.Bytes(), tag.Value.Bytes()

		var clonedName, clonedValue []byte
		if idx := bytes.Index(clonedID, nameBytes); idx != -1 {
			clonedName = clonedID[idx : idx+len(nameBytes)]
		} else {
			clonedName = append([]byte(nil), nameBytes...)
		}
		if idx := bytes.Index(clonedID, valueBytes); idx != -1 {
			clonedValue = clonedID[idx : idx+len(valueBytes)]
		} else {
			clonedValue = append([]byte(nil), valueBytes...)
		}

		fields = append(fields, doc.Field{
			Name:  clonedName,
			Value: clonedValue,
		})
	}
	if err := tags.Err(); err != nil {
		return doc.Document{}, err
	}
	return doc.Document{
		ID:     clonedID,
		Fields: fields,
	}, nil
}

// FromMetricIterNoClone converts the provided metric id+tags iterator into a
// document without cloning.
func FromMetricIterNoClone(id ident.ID, tags ident.TagIterator) (doc.Document, error) {
	fields := make([]doc.Field, 0, tags.Remaining())
	for tags.Next() {
		tag := tags.Current()
		if bytes.Equal(ReservedFieldNameID, tag.Name.Bytes()) {
			return doc.Document{}, ErrUsingReservedFieldName
		}
		fields = append(fields, doc.Field{
			Name:  tag.Name.Bytes(),
			Value: tag.Value.Bytes(),
		})
	}
	if err := tags.Err(); err != nil {
		return doc.Document{}, err
	}
	return doc.Document{
		ID:     id.Bytes(),
		Fields: fields,
	}, nil
}

// TagsFromTagsIter returns an ident.Tags from a TagIterator. It also tries
// to re-use bytes from the seriesID if they're also present in the tags
// instead of re-allocating them. This requires that the ident.Tags that is
// returned will have the same (or shorter) life time as the seriesID,
// otherwise the operation is unsafe.
func TagsFromTagsIter(
	seriesID ident.ID,
	iter ident.TagIterator,
	idPool ident.Pool,
) (ident.Tags, error) {
	var tags ident.Tags
	if idPool != nil {
		tags = idPool.Tags()
	} else {
		tagSlice := make([]ident.Tag, 0, iter.Len())
		tags = ident.NewTags(tagSlice...)
	}

	seriesIDBytes := ident.BytesID(seriesID.Bytes())
	for iter.Next() {
		curr := iter.Current()

		var (
			nameBytes, valueBytes = curr.Name.Bytes(), curr.Value.Bytes()
			tag                   ident.Tag
			idRef                 bool
		)
		if idx := bytes.Index(seriesIDBytes, nameBytes); idx != -1 {
			tag.Name = seriesIDBytes[idx : idx+len(nameBytes)]
			idRef = true
		} else {
			if idPool != nil {
				tag.Name = idPool.Clone(curr.Name)
			} else {
				copiedBytes := append([]byte(nil), curr.Name.Bytes()...)
				tag.Name = ident.BytesID(copiedBytes)
			}
		}
		if idx := bytes.Index(seriesIDBytes, valueBytes); idx != -1 {
			tag.Value = seriesIDBytes[idx : idx+len(valueBytes)]
			idRef = true
		} else {
			if idPool != nil {
				tag.Value = idPool.Clone(curr.Value)
			} else {
				copiedBytes := append([]byte(nil), curr.Value.Bytes()...)
				tag.Value = ident.BytesID(copiedBytes)
			}
		}

		if idRef {
			tag.NoFinalize() // Taken ref, cannot finalize this.
		}

		tags.Append(tag)
	}

	if err := iter.Err(); err != nil {
		return ident.Tags{}, err
	}
	return tags, nil
}

// NB(prateek): we take an independent copy of the bytes underlying
// any ids provided, as we need to maintain the lifecycle of the indexed
// bytes separately from the rest of the storage subsystem.
func clone(id ident.ID) []byte {
	original := id.Bytes()
	clone := make([]byte, len(original))
	copy(clone, original)
	return clone
}

// Opts are the pools required for conversions.
type Opts struct {
	IdentPool        ident.Pool
	CheckedBytesPool pool.CheckedBytesPool
}

// wrapBytes wraps the provided bytes into an ident.ID backed by pooled types,
// such that calling Finalize() on the returned type returns the resources to
// the pools.
func (o Opts) wrapBytes(b []byte) ident.ID {
	cb := o.CheckedBytesPool.Get(len(b))
	cb.IncRef()
	cb.AppendAll(b)
	id := o.IdentPool.BinaryID(cb)
	// release held reference so now the only reference to the bytes is owned by `id`
	cb.DecRef()
	return id
}

// ToMetric converts the provided doc to metric id+tags.
func ToMetric(d doc.Document, opts Opts) (ident.ID, ident.TagIterator, error) {
	if len(d.ID) == 0 {
		return nil, nil, errInvalidResultMissingID
	}
	return opts.wrapBytes(d.ID), newTagIter(d, opts), nil
}

// tagIter exposes an ident.TagIterator interface over a doc.Document.
type tagIter struct {
	docFields doc.Fields

	err        error
	done       bool
	currentIdx int
	currentTag ident.Tag

	opts Opts
}

// NB: force tagIter to implement the ident.TagIterator interface.
var _ ident.TagIterator = &tagIter{}

func newTagIter(d doc.Document, opts Opts) ident.TagIterator {
	return &tagIter{
		docFields:  d.Fields,
		currentIdx: -1,
		opts:       opts,
	}
}

func (t *tagIter) Next() bool {
	if t.err != nil || t.done {
		return false
	}
	hasNext := t.parseNext()
	if !hasNext {
		t.done = true
	}
	return hasNext
}

func (t *tagIter) parseNext() (hasNext bool) {
	t.releaseCurrent()
	t.currentIdx++
	// early terminate if we know there's no more fields
	if t.currentIdx >= len(t.docFields) {
		return false
	}
	// if there are fields, we have to ensure the next field
	// is not using the reserved ID fieldname
	next := t.docFields[t.currentIdx]
	if bytes.Equal(ReservedFieldNameID, next.Name) {
		t.err = ErrUsingReservedFieldName
		return false
	}
	// otherwise, we're good.
	t.currentTag = ident.Tag{
		Name:  t.opts.wrapBytes(next.Name),
		Value: t.opts.wrapBytes(next.Value),
	}
	return true
}

func (t *tagIter) releaseCurrent() {
	if t.currentTag.Name != nil {
		t.currentTag.Name.Finalize()
		t.currentTag.Name = nil
	}
	if t.currentTag.Value != nil {
		t.currentTag.Value.Finalize()
		t.currentTag.Value = nil
	}
}

func (t *tagIter) Current() ident.Tag {
	return t.currentTag
}

func (t *tagIter) CurrentIndex() int {
	if t.currentIdx >= 0 {
		return t.currentIdx
	}
	return 0
}

func (t *tagIter) Err() error {
	return t.err
}

func (t *tagIter) Close() {
	t.releaseCurrent()
	t.done = true
}

func (t *tagIter) Len() int {
	return len(t.docFields)
}

func (t *tagIter) Remaining() int {
	l := len(t.docFields) - (t.currentIdx + 1)
	return l
}

func (t *tagIter) Duplicate() ident.TagIterator {
	var dupe = *t
	if t.currentTag.Name != nil {
		dupe.currentTag = t.opts.IdentPool.CloneTag(t.currentTag)
	}
	return &dupe
}
