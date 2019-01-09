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

package models

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"
)

var (
	defaultOptions = NewTagOptions()
)

// NewTags builds a tags with the given size and tag options.
func NewTags(size int, opts TagOptions) Tags {
	if opts == nil {
		opts = defaultOptions
	}

	return Tags{
		// Todo: Pool these
		Tags: make([]Tag, 0, size),
		Opts: opts,
	}
}

// EmptyTags returns empty tags with a default tag options.
func EmptyTags() Tags {
	return NewTags(0, nil)
}

// ID returns a byte slice representation of the tags.
func (t Tags) ID() []byte {
	// TODO: pool these bytes.
	id := make([]byte, t.idLen())
	idx := -1

	for _, tag := range t.Tags {
		idx += copy(id[idx+1:], tag.Name) + 1
		id[idx] = eq
		idx += copy(id[idx+1:], tag.Value) + 1
		id[idx] = sep
	}

	return id
}

// idLen returns the length of the ID that would be generated from the tags.
func (t Tags) idLen() int {
	idLen := 0 // 2 * t.Len() // account for eq and sep
	for _, tag := range t.Tags {
		idLen += len(tag.Name)
		idLen += len(tag.Value)
	}

	return idLen
}

// ID returns a byte slice representation of the tags.
func (t Tags) newid() []byte {
	// TODO: pool these bytes.
	id := make([]byte, t.idLen())
	idx := -1

	for _, tag := range t.Tags {
		idx += copy(id[idx+1:], tag.Name) + 1
		id[idx] = eq
		idx += copy(id[idx+1:], tag.Value) + 1
		id[idx] = sep
	}

	return id
}

func tagLengthsToBytes(tagLengths []int) []byte {
	// account for tag length seperator and final character,
	// and at least a single byte for the length
	byteLength := 1 + len(tagLengths)
	for _, l := range tagLengths {
		for ; l > 0; l >>= 8 {
			byteLength++
		}
	}

	tagBytes := make([]byte, byteLength)
	idx := 0
	for _, l := range tagLengths {
		for ; l > 0; l >>= 8 {
			tagBytes[idx] = byte(l % 256)
			idx++
		}
		idx++
	}

	return tagBytes
}

// idLen returns the length of the ID that would be generated from the tags.
func (t Tags) newlen() (int, []int) {
	idLen := 0 // t.Len() // 2 * t.Len()  // account for eq and sep
	tagLengths := make([]int, len(t.Tags))
	for i, tag := range t.Tags {
		tagLen := len(tag.Name) + 1
		tagLen += len(tag.Value)
		tagLengths[i] = tagLen
		idLen += tagLen
	}

	return idLen, tagLengths
}

// IDWithExcludes returns a string representation of the tags excluding some tag keys.
func (t Tags) IDWithExcludes(excludeKeys ...[]byte) uint64 {
	b := make([]byte, 0, t.Len())
	for _, tag := range t.Tags {
		// Always exclude the metric name by default
		if bytes.Equal(tag.Name, t.Opts.MetricName()) {
			continue
		}

		found := false
		for _, n := range excludeKeys {
			if bytes.Equal(n, tag.Name) {
				found = true
				break
			}
		}

		// Skip the key
		if found {
			continue
		}

		b = append(b, tag.Name...)
		b = append(b, eq)
		b = append(b, tag.Value...)
		b = append(b, sep)
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

func (t Tags) tagSubset(keys [][]byte, include bool) Tags {
	tags := NewTags(t.Len(), t.Opts)
	for _, tag := range t.Tags {
		found := false
		for _, k := range keys {
			if bytes.Equal(tag.Name, k) {
				found = true
				break
			}
		}

		if found == include {
			tags = tags.AddTag(tag)
		}
	}

	return tags
}

// TagsWithoutKeys returns only the tags which do not have the given keys.
func (t Tags) TagsWithoutKeys(excludeKeys [][]byte) Tags {
	return t.tagSubset(excludeKeys, false)
}

// IDWithKeys returns a string representation of the tags only including the given keys.
func (t Tags) IDWithKeys(includeKeys ...[]byte) uint64 {
	b := make([]byte, 0, t.Len())
	for _, tag := range t.Tags {
		for _, k := range includeKeys {
			if bytes.Equal(tag.Name, k) {
				b = append(b, tag.Name...)
				b = append(b, eq)
				b = append(b, tag.Value...)
				b = append(b, sep)
				break
			}
		}
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// TagsWithKeys returns only the tags which have the given keys.
func (t Tags) TagsWithKeys(includeKeys [][]byte) Tags {
	return t.tagSubset(includeKeys, true)
}

// WithoutName copies the tags excluding the name tag.
func (t Tags) WithoutName() Tags {
	return t.TagsWithoutKeys([][]byte{t.Opts.MetricName()})
}

// Get returns the value for the tag with the given name.
func (t Tags) Get(key []byte) ([]byte, bool) {
	for _, tag := range t.Tags {
		if bytes.Equal(tag.Name, key) {
			return tag.Value, true
		}
	}

	return nil, false
}

// Clone returns a copy of the tags.
func (t Tags) Clone() Tags {
	// TODO: Pool these
	clonedTags := make([]Tag, t.Len())
	for i, tag := range t.Tags {
		clonedTags[i] = tag.Clone()
	}

	return Tags{
		Tags: clonedTags,
		Opts: t.Opts,
	}
}

// AddTag is used to add a single tag and maintain sorted order.
func (t Tags) AddTag(tag Tag) Tags {
	t.Tags = append(t.Tags, tag)
	return t.Normalize()
}

// SetName sets the metric name.
func (t Tags) SetName(value []byte) Tags {
	return t.AddOrUpdateTag(Tag{Name: t.Opts.MetricName(), Value: value})
}

// Name gets the metric name.
func (t Tags) Name() ([]byte, bool) {
	return t.Get(t.Opts.MetricName())
}

// AddTags is used to add a list of tags and maintain sorted order.
func (t Tags) AddTags(tags []Tag) Tags {
	t.Tags = append(t.Tags, tags...)
	return t.Normalize()
}

// AddOrUpdateTag is used to add a single tag and maintain sorted order,
// or to replace the value of an existing tag.
func (t Tags) AddOrUpdateTag(tag Tag) Tags {
	tags := t.Tags
	for i, tt := range tags {
		if bytes.Equal(tag.Name, tt.Name) {
			tags[i].Value = tag.Value
			return t
		}
	}

	return t.AddTag(tag)
}

// Add is used to add two tag structures and maintain sorted order.
func (t Tags) Add(other Tags) Tags {
	t.Tags = append(t.Tags, other.Tags...)
	return t.Normalize()
}

func (t Tags) Len() int      { return len(t.Tags) }
func (t Tags) Swap(i, j int) { t.Tags[i], t.Tags[j] = t.Tags[j], t.Tags[i] }
func (t Tags) Less(i, j int) bool {
	return bytes.Compare(t.Tags[i].Name, t.Tags[j].Name) == -1
}

// Normalize normalizes the tags by sorting them in place.
// In the future, it might also ensure other things like uniqueness.
func (t Tags) Normalize() Tags {
	sort.Sort(t)
	return t
}

func (t Tag) String() string {
	return fmt.Sprintf("%s: %s", t.Name, t.Value)
}

// Clone returns a copy of the tag.
func (t Tag) Clone() Tag {
	// Todo: Pool these
	clonedName := make([]byte, len(t.Name))
	clonedVal := make([]byte, len(t.Value))
	copy(clonedName, t.Name)
	copy(clonedVal, t.Value)
	return Tag{
		Name:  clonedName,
		Value: clonedVal,
	}
}
