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

	"github.com/m3db/m3/src/query/util/writer"

	"github.com/m3db/m3/src/query/models/strconv"
	"github.com/m3db/m3/src/x/ident"
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
// strategy from the tag options.
func (t Tags) ID() []byte {
	// TODO: pool these bytes
	var bytes []byte
	return tagsID(bytes, t.Opts, t, nil)
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

// AddTagWithoutNormalizing is used to add a single tag.
func (t Tags) AddTagWithoutNormalizing(tag Tag) Tags {
	t.Tags = append(t.Tags, tag)
	return t
}

// SetName sets the metric name.
func (t Tags) SetName(value []byte) Tags {
	return t.AddOrUpdateTag(Tag{Name: t.Opts.MetricName(), Value: value})
}

// Name gets the metric name.
func (t Tags) Name() ([]byte, bool) {
	return t.Get(t.Opts.MetricName())
}

// SetBucket sets the bucket tag value.
func (t Tags) SetBucket(value []byte) Tags {
	return t.AddOrUpdateTag(Tag{Name: t.Opts.BucketName(), Value: value})
}

// Bucket gets the bucket tag value.
func (t Tags) Bucket() ([]byte, bool) {
	return t.Get(t.Opts.BucketName())
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

type sortableTagsNumericallyAsc Tags

func (t sortableTagsNumericallyAsc) Len() int { return len(t.Tags) }
func (t sortableTagsNumericallyAsc) Swap(i, j int) {
	t.Tags[i], t.Tags[j] = t.Tags[j], t.Tags[i]
}
func (t sortableTagsNumericallyAsc) Less(i, j int) bool {
	iName, jName := t.Tags[i].Name, t.Tags[j].Name
	lenDiff := len(iName) - len(jName)
	if lenDiff < 0 {
		return true
	}

	if lenDiff > 0 {
		return false
	}

	return bytes.Compare(iName, jName) == -1
}

// Normalize normalizes the tags by sorting them in place.
// In the future, it might also ensure other things like uniqueness.
func (t Tags) Normalize() Tags {
	// Graphite tags are sorted numerically rather than lexically.
	if t.Opts.IDSchemeType() == TypeGraphite {
		sort.Sort(sortableTagsNumericallyAsc(t))
	} else {
		sort.Sort(t)
	}

	return t
}

// HashedID returns the hashed ID for the tags.
func (t Tags) HashedID() uint64 {
	h := fnv.New64a()
	h.Write(t.ID())
	return h.Sum64()
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

// TagsIDIdentTagIterator returns a tag ID from a
// ident.TagIterator.
// Note: Does not close the ident.TagIterator, up to caller
// to do that.
func TagsIDIdentTagIterator(
	dst []byte,
	src ident.TagIterator,
	opts TagOptions,
) ([]byte, error) {
	dst = tagsID(dst, opts, Tags{}, src)
	if err := src.Err(); err != nil {
		return nil, err
	}
	return dst, nil
}

// tagsID returns an ID, note: must check it.Err() after if using iterator.
func tagsID(dst []byte, opts TagOptions, t Tags, it ident.TagIterator) []byte {
	schemeType := opts.IDSchemeType()
	if len(t.Tags) == 0 {
		if schemeType == TypeQuoted {
			return []byte("{}")
		}

		return []byte("")
	}

	switch schemeType {
	case TypeLegacy:
		return legacyID(dst, t, it)
	case TypeQuoted:
		return quotedID(dst, t, it)
	case TypePrependMeta:
		return prependMetaID(dst, t, it)
	case TypeGraphite:
		return graphiteID(dst, t, it)
	default:
		// Default to prepending meta
		return prependMetaID(dst, t, it)
	}
}

// legacyID returns a legacy ID, note: must check it.Err() after if using iterator.
func legacyID(dst []byte, t Tags, it ident.TagIterator) []byte {
	dst = dst[:0]
	if it != nil {
		for it.Next() {
			tag := it.Current()
			dst = append(dst, tag.Name.Bytes()...)
			dst = append(dst, eq)
			dst = append(dst, tag.Value.Bytes()...)
			dst = append(dst, sep)
		}
	} else {
		for _, tag := range t.Tags {
			dst = append(dst, tag.Name...)
			dst = append(dst, eq)
			dst = append(dst, tag.Value...)
			dst = append(dst, sep)
		}
	}

	return dst
}

type tagEscaping struct {
	escapeName  bool
	escapeValue bool
}

// quotedID returns a quote ID, note: must check it.Err() after if using iterator.
func quotedID(dst []byte, t Tags, it ident.TagIterator) []byte {
	var (
		idLen        int
		needEscaping []tagEscaping
		l            int
		escape       tagEscaping
	)

	if it != nil {
		duplicate := it.Duplicate()
		numTags := duplicate.Remaining()

		for i := 0; duplicate.Next(); i++ {
			curr := duplicate.Current()
			l, escape = serializedLength(curr.Name.Bytes(), curr.Value.Bytes())
			idLen += l
			if escape.escapeName || escape.escapeValue {
				if needEscaping == nil {
					needEscaping = make([]tagEscaping, numTags)
				}

				needEscaping[i] = escape
			}
		}
		duplicate.Close()
	} else {
		for i, tt := range t.Tags {
			l, escape = serializedLength(tt.Name, tt.Value)
			idLen += l
			if escape.escapeName || escape.escapeValue {
				if needEscaping == nil {
					needEscaping = make([]tagEscaping, len(t.Tags))
				}

				needEscaping[i] = escape
			}
		}
	}

	tagLength := 2 * len(t.Tags)
	idLen += tagLength + 1 // account for separators and brackets

	// Ensure has capacity
	if cap(dst) < idLen {
		dst = make([]byte, 0, idLen)
	}

	if needEscaping == nil {
		return quoteIDSimple(dst, idLen, t, it)
	}

	if it != nil {
		// Extend as required.
		dst = dst[:idLen]
		dst[0] = leftBracket
		idx := 1
		for i := 0; it.Next(); i++ {
			tag := it.Current()
			idx = writeAtIndex(dst, tag.Name.Bytes(), tag.Value.Bytes(), needEscaping[i], idx)
			if it.Remaining() != 0 {
				dst[idx] = sep
				idx++
			}
		}
		dst[idx] = rightBracket
		// Take the length.
		dst = dst[:idx+1]
	} else {
		// Extend as required.
		dst = dst[:idLen]
		lastIndex := len(t.Tags) - 1
		dst[0] = leftBracket
		idx := 1
		for i, tt := range t.Tags[:lastIndex] {
			idx = writeAtIndex(dst, tt.Name, tt.Value, needEscaping[i], idx)
			dst[idx] = sep
			idx++
		}

		tt := t.Tags[lastIndex]
		idx = writeAtIndex(dst, tt.Name, tt.Value, needEscaping[lastIndex], idx)
		dst[idx] = rightBracket
		// Take the length.
		dst = dst[:idx+1]
	}

	return dst
}

// quoteIDSimple returns a simple quote ID, note: must check it.Err() after if using iterator.
func quoteIDSimple(dst []byte, length int, t Tags, it ident.TagIterator) []byte {
	// Ensure has capacity
	if cap(dst) < length {
		dst = make([]byte, 0, length)
	}

	if it != nil {
		dst = append(dst, leftBracket)
		for it.Next() {
			curr := it.Current()
			dst = append(dst, curr.Name.Bytes()...)
			dst = append(dst, eq)

			tagValue := curr.Value.Bytes()
			// The QuoteSimple api requires pre-extended slice
			newLen := len(dst) + len(tagValue) + 2
			_ = strconv.QuoteSimple(dst[:newLen], tagValue, len(dst))
			dst = dst[:newLen]

			// Check if separator needs appending
			if it.Remaining() != 0 {
				dst = append(dst, sep)
			}
		}
		dst = append(dst, rightBracket)
	} else {
		dst = dst[0:length]
		dst[0] = leftBracket
		idx := 1
		lastIndex := len(t.Tags) - 1
		for _, tag := range t.Tags[:lastIndex] {
			idx += copy(dst[idx:], tag.Name)
			dst[idx] = eq
			idx++
			idx = strconv.QuoteSimple(dst, tag.Value, idx)
			dst[idx] = sep
			idx++
		}
		tag := t.Tags[lastIndex]
		idx += copy(dst[idx:], tag.Name)
		dst[idx] = eq
		idx++
		idx = strconv.QuoteSimple(dst, tag.Value, idx)
		dst[idx] = rightBracket
	}

	return dst
}

func serializedLength(name, value []byte) (int, tagEscaping) {
	var (
		idLen    int
		escaping tagEscaping
	)
	if strconv.NeedToEscape(name) {
		idLen += strconv.EscapedLength(name)
		escaping.escapeName = true
	} else {
		idLen += len(name)
	}

	if strconv.NeedToEscape(value) {
		idLen += strconv.QuotedLength(value)
		escaping.escapeValue = true
	} else {
		idLen += len(value) + 2
	}

	return idLen, escaping
}

func writeAtIndex(id, name, value []byte, escape tagEscaping, idx int) int {
	if escape.escapeName {
		idx = strconv.Escape(id, name, idx)
	} else {
		idx += copy(id[idx:], name)
	}

	// add = character
	id[idx] = eq
	idx++

	if escape.escapeValue {
		idx = strconv.Quote(id, value, idx)
	} else {
		idx = strconv.QuoteSimple(id, value, idx)
	}

	return idx
}

func prependMetaID(dst []byte, t Tags, it ident.TagIterator) []byte {
	length, metaLengths := prependMetaLen(t, it)

	// Ensure has capacity
	if cap(dst) < length {
		dst = make([]byte, length)
	} else {
		dst = dst[:length]
	}

	idx := writeTagLengthMeta(dst, metaLengths)
	if it != nil {
		for it.Next() {
			tag := it.Current()
			idx += copy(dst[idx:], tag.Name.Bytes())
			idx += copy(dst[idx:], tag.Value.Bytes())
		}
	} else {
		for _, tag := range t.Tags {
			idx += copy(dst[idx:], tag.Name)
			idx += copy(dst[idx:], tag.Value)
		}
	}

	return dst[:idx]
}

func writeTagLengthMeta(dst []byte, lengths []int) int {
	idx := writer.WriteIntegers(dst, lengths, sep, 0)
	dst[idx] = finish
	return idx + 1
}

func prependMetaLen(t Tags, it ident.TagIterator) (int, []int) {
	idLen := 1 // account for separator

	var tagLengths []int
	if it != nil {
		duplicate := it.Duplicate()
		tagLengths = make([]int, duplicate.Remaining()*2)
		for i := 0; duplicate.Next(); i++ {
			tag := duplicate.Current()
			tagLen := len(tag.Name.Bytes())
			tagLengths[2*i] = tagLen
			idLen += tagLen
			tagLen = len(tag.Value.Bytes())
			tagLengths[2*i+1] = tagLen
			idLen += tagLen
		}
		duplicate.Close()
	} else {
		tagLengths = make([]int, len(t.Tags)*2)
		for i, tag := range t.Tags {
			tagLen := len(tag.Name)
			tagLengths[2*i] = tagLen
			idLen += tagLen
			tagLen = len(tag.Value)
			tagLengths[2*i+1] = tagLen
			idLen += tagLen
		}
	}

	prefixLen := writer.IntsLength(tagLengths)
	return idLen + prefixLen, tagLengths
}

func graphiteID(dst []byte, t Tags, it ident.TagIterator) []byte {
	length := graphiteIDLen(t, it)

	if it != nil {
		for it.Next() {
			tag := it.Current()
			dst = append(dst, tag.Value.Bytes()...)
			if it.Remaining() != 0 {
				dst = append(dst, graphiteSep)
			}
		}
	} else {
		// Ensure has capacity
		if cap(dst) < length {
			dst = make([]byte, length)
		} else {
			dst = dst[:length]
		}

		idx := 0
		lastIndex := len(t.Tags) - 1
		for _, tag := range t.Tags[:lastIndex] {
			idx += copy(dst[idx:], tag.Value)
			dst[idx] = graphiteSep
			idx++
		}

		idx += copy(dst[idx:], t.Tags[lastIndex].Value)
		dst = dst[:idx]
	}

	return dst
}

func graphiteIDLen(t Tags, it ident.TagIterator) int {
	var idLen int
	if it != nil {
		duplicate := it.Duplicate()
		idLen = duplicate.Remaining() - 1 // account for separators
		for duplicate.Next() {
			curr := duplicate.Current()
			idLen += len(curr.Value.Bytes())
		}
		duplicate.Close()
	} else {
		idLen = t.Len() - 1 // account for separators
		for _, tag := range t.Tags {
			idLen += len(tag.Value)
		}
	}

	return idLen
}
