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

// Package ident provides utilities for working with identifiers.
package ident

import (
	"fmt"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
)

// ID represents an immutable identifier to allow use of byte slice pooling
// for the contents of the ID.
type ID interface {
	fmt.Stringer

	Data() checked.Bytes
	Bytes() []byte
	Equal(value ID) bool
	Reset()
	Finalize()
}

// TagName represents the name of a timeseries tag.
type TagName ID

// TagValue represents the value of a timeseries tag.
type TagValue ID

// Tag represents a timeseries tag.
type Tag struct {
	Name  TagName
	Value TagValue
}

// Finalize releases all resources held by the Tag.
func (t *Tag) Finalize() {
	if t.Name != nil {
		t.Name.Finalize()
		t.Name = nil
	}
	if t.Value != nil {
		t.Value.Finalize()
		t.Value = nil
	}
}

// Equal returns whether the two tags are equal.
func (t Tag) Equal(value Tag) bool {
	return t.Name.Equal(value.Name) && t.Value.Equal(value.Value)
}

// Pool represents an automatic pool of `ident` objects.
type Pool interface {
	// GetBinaryID will create a new binary ID and take reference to the bytes.
	// When the context closes the ID will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryID(c context.Context, data checked.Bytes) ID

	// BinaryID will create a new binary ID and take a reference to the bytes.
	BinaryID(data checked.Bytes) ID

	// GetBinaryTag will create a new binary Tag and take reference to the bytes.
	// When the context closes, the Tag will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryTag(c context.Context, name, value checked.Bytes) Tag

	// BinaryTag will create a new binary Tag and take a reference to the provided bytes.
	BinaryTag(name, value checked.Bytes) Tag

	// GetStringID will create a new string ID and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringID(c context.Context, id string) ID

	// StringID will create a new string ID and create a bytes copy of the
	// string.
	StringID(data string) ID

	// GetStringTag will create a new string Tag and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringTag(c context.Context, name, value string) Tag

	// StringTag will create a new string Tag and create a bytes copy of the
	// string.
	StringTag(name, value string) Tag

	// Put an ID back in the pool.
	Put(id ID)

	// PutTag puts a tag back in the pool.
	PutTag(tag Tag)

	// Clone replicates a given ID into a pooled ID.
	Clone(id ID) ID

	// CloneTag replicates a given Tag into a pooled Tag.
	CloneTag(tag Tag) Tag
}

// Iterator represents an iterator over `ID` instances. It is not thread-safe.
type Iterator interface {
	// Next returns a bool indicating the presence of the next ID instance.
	Next() bool

	// Current returns the current ID instance.
	Current() ID

	// Close releases any resources held by the iterator.
	Close()

	// Err returns any errors encountered during iteration.
	Err() error

	// Remaining returns the number of elements remaining to be iterated over.
	Remaining() int

	// Dupe returns an independent duplicate of the iterator.
	Duplicate() Iterator
}

// TagIterator represents an iterator over `Tag` instances. It is not thread-safe.
type TagIterator interface {
	// Next returns a bool indicating the presence of the next Tag instance.
	Next() bool

	// Current returns the current Tag instance.
	Current() Tag

	// Err returns any errors encountered during iteration.
	Err() error

	// Close releases any resources held by the iterator.
	Close()

	// Remaining returns the number of elements remaining to be iterated over.
	Remaining() int

	// Dupe returns an independent duplicate of the iterator.
	Duplicate() TagIterator
}

// TagSliceIterator represents a TagIterator that can be reset with a slice
// of tags.  It is not thread-safe.
type TagSliceIterator interface {
	TagIterator

	// Reset allows the tag iterator to be reused with a new set of tags.
	Reset(tags Tags)
}

// IDs is a collection of ID instances.
type IDs []ID

// Finalize finalizes all IDs.
func (ids IDs) Finalize() {
	for _, id := range ids {
		id.Finalize()
	}
}

// Tags is a collection of Tag instances.
type Tags []Tag

// Finalize finalizes all Tags.
func (tags Tags) Finalize() {
	for i := range tags {
		t := tags[i]
		t.Finalize()
	}
}

// Equal returns a bool indicating if the tags are equal. It requires
// the two slices are ordered the same.
func (tags Tags) Equal(other Tags) bool {
	if len(tags) != len(other) {
		return false
	}
	for i := 0; i < len(tags); i++ {
		equal := tags[i].Name.Equal(other[i].Name) && tags[i].Value.Equal(other[i].Value)
		if !equal {
			return false
		}
	}
	return true
}
