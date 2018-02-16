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
	"crypto/md5"
	"fmt"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"

	"github.com/spaolacci/murmur3"
)

// ID represents an immutable identifier for a timeseries.
type ID interface {
	fmt.Stringer

	Data() checked.Bytes
	Hash() Hash
	Equal(value ID) bool

	Finalize()
	Reset()
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

// Pool represents an automatic pool of `ident` objects.
type Pool interface {
	// GetBinaryID will create a new binary ID and take reference to the bytes.
	// When the context closes the ID will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryID(context.Context, checked.Bytes) ID

	// GetBinaryTag will create a new binary Tag and take reference to the bytes.
	// When the context closes, the Tag will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryTag(c context.Context, name checked.Bytes, value checked.Bytes) Tag

	// GetStringID will create a new string ID and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringID(c context.Context, id string) ID

	// GetStringTag will create a new string Tag and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringTag(c context.Context, name string, value string) Tag

	// Put an ID back in the pool.
	Put(ID)

	// PutTag puts a tag back in the pool.
	PutTag(Tag)

	// Clone replicates a given ID into a pooled ID.
	Clone(other ID) ID

	// CloneIDs replicates the given IDs into pooled IDs.
	CloneIDs(Iterator) IDs
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

	// Clone returns an independent clone of the iterator.
	Clone() Iterator
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

	// Clone returns an independent clone of the iterator.
	Clone() TagIterator
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
	for _, t := range tags {
		t.Name.Finalize()
		t.Value.Finalize()
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

// Hash represents a form of ID suitable to be used as map keys.
type Hash [md5.Size]byte

// HashFn is the default hashing implementation for IDs.
func HashFn(data []byte) Hash {
	return md5.Sum(data)
}

// Hash128 is a 128-bit hash of an ID consisting of two unsigned 64-bit ints.
type Hash128 [2]uint64

// Murmur3Hash128 computes the 128-bit hash of an id.
func Murmur3Hash128(data []byte) Hash128 {
	h0, h1 := murmur3.Sum128(data)
	return Hash128{h0, h1}
}
