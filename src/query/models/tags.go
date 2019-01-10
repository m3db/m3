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
	"sort"

	"github.com/m3db/m3/src/query/models/strconv"
)

// NewTags builds a tags with the given size and tag options.
func NewTags(size int, opts TagOptions) Tags {
	if opts == nil {
		opts = NewTagOptions()
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

// ID returns a byte slice representation of the tags, using the generation
// strategy from .
func (t Tags) ID() []byte {
	schemeType := t.Opts.IDSchemeType()
	switch schemeType {
	case TypeLegacy:
		return t.legacyID()
	case TypeQuoted:
		return t.quotedID()
	case TypePrependMeta:
		return t.prependMetaID()
	default:
		// Default to prepending meta
		return t.prependMetaID()
	}
}

func (t Tags) legacyID() []byte {
	// TODO: pool these bytes.
	id := make([]byte, t.legacyIDLen())
	idx := -1
	for _, tag := range t.Tags {
		idx += copy(id[idx+1:], tag.Name) + 1
		id[idx] = eq
		idx += copy(id[idx+1:], tag.Value) + 1
		id[idx] = sep
	}

	return id
}

func (t Tags) legacyIDLen() int {
	idLen := 2 * t.Len() // account for eq and sep
	for _, tag := range t.Tags {
		idLen += len(tag.Name)
		idLen += len(tag.Value)
	}

	return idLen
}

func (t Tags) quotedID() []byte {
	// TODO: pool these bytes.
	id := make([]byte, t.quotedIDLen())
	idx := 0
	for _, tag := range t.Tags {
		idx += copy(id[idx:], tag.Name)
		idx += strconv.Quote(id[idx:], tag.Value)
	}

	return id
}

// newIdLen returns the length of the ID that would be generated from the tags.
func (t Tags) quotedIDLen() int {
	idLen := 0
	for _, tag := range t.Tags {
		idLen += len(tag.Name)
		idLen += strconv.QuotedLength(tag.Value)
	}

	return idLen
}

// ID returns a byte slice representation of the tags.
func (t Tags) prependMetaID() []byte {
	l, metaLengths := t.prependMetaLen()
	// TODO: pool these bytes.
	id := make([]byte, l)
	idx := prependTagLengthMeta(id, metaLengths)
	for _, tag := range t.Tags {
		idx += copy(id[idx:], tag.Name)
		idx += copy(id[idx:], tag.Value)
	}

	return id
}

func prependTagLengthMeta(dst []byte, lengths []int) int {
	idx := 0
	for _, l := range lengths {
		for ; l > 0; l >>= 8 {
			dst[idx] = byte(l % 256)
			idx++
		}
	}

	return idx
}

// idLen returns the length of the ID that would be generated from the tags.
func (t Tags) prependMetaLen() (int, []int) {
	idLen := 0
	tagLengths := make([]int, len(t.Tags))
	for i, tag := range t.Tags {
		tagLen := len(tag.Name)
		tagLen += len(tag.Value)
		tagLengths[i] = tagLen
		idLen += tagLen
	}

	byteLength := len(tagLengths)
	for _, l := range tagLengths {
		for l >>= 8; l > 0; l >>= 8 {
			byteLength++
		}
	}

	return idLen + byteLength, tagLengths
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
