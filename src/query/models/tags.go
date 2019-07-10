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
	return tagsID(bytes, newTagsIter(t, nil), t.Opts)
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
	dst = tagsID(dst, newTagsIter(Tags{}, src), opts)
	if err := src.Err(); err != nil {
		return nil, err
	}
	return dst, nil
}

type tagsIter struct {
	t       Tags
	it      ident.TagIterator
	idx     int
	numTags int
}

func newTagsIter(
	t Tags,
	it ident.TagIterator,
) tagsIter {
	if it == nil {
		it = ident.EmptyTagIterator
	}
	return tagsIter{
		t:       t,
		it:      it,
		idx:     -1,
		numTags: t.Len() + it.Remaining(),
	}
}

func (i tagsIter) Restart() tagsIter {
	r := i
	r.idx = -1
	r.it.Restart()
	return r
}

func (i tagsIter) Index() int {
	return i.idx
}

func (i tagsIter) Len() int {
	return i.numTags
}

func (i tagsIter) Next() (tagsIter, Tag, bool) {
	r := i
	r.idx++
	hasNext := r.idx < r.numTags
	if !hasNext {
		return r, Tag{}, false
	}
	if r.idx < r.t.Len() {
		return r, r.t.Tags[r.idx], true
	}

	r.it.Next()
	curr := r.it.Current()
	tag := Tag{Name: curr.Name.Bytes(), Value: curr.Value.Bytes()}
	return r, tag, true
}

func (i tagsIter) Close() {
	i.it.Close()
}

// tagsID returns an ID, note: must check it.Err() after if using iterator.
func tagsID(dst []byte, t tagsIter, opts TagOptions) []byte {
	schemeType := opts.IDSchemeType()
	if t.Len() == 0 {
		if schemeType == TypeQuoted {
			return []byte("{}")
		}

		return []byte("")
	}

	switch schemeType {
	case TypeLegacy:
		return legacyID(dst, t)
	case TypeQuoted:
		return quotedID(dst, t)
	case TypePrependMeta:
		return prependMetaID(dst, t)
	case TypeGraphite:
		return graphiteID(dst, t)
	default:
		// Default to prepending meta
		return prependMetaID(dst, t)
	}
}

// legacyID returns a legacy ID, note: must check it.Err() after if using iterator.
func legacyID(dst []byte, t tagsIter) []byte {
	dst = dst[:0]

	t = t.Restart()
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		dst = append(dst, tag.Name...)
		dst = append(dst, eq)
		dst = append(dst, tag.Value...)
		dst = append(dst, sep)
	}

	return dst
}

type tagEscaping struct {
	escapeName  bool
	escapeValue bool
}

// quotedID returns a quote ID, note: must check it.Err() after if using iterator.
func quotedID(dst []byte, t tagsIter) []byte {
	var (
		idLen        int
		needEscaping []tagEscaping
		l            int
		escape       tagEscaping
	)

	t = t.Restart()
	for t, tt, next := t.Next(); next; t, tt, next = t.Next() {
		l, escape = serializedLength(tt.Name, tt.Value)
		idLen += l
		if escape.escapeName || escape.escapeValue {
			if needEscaping == nil {
				needEscaping = make([]tagEscaping, t.Len())
			}

			needEscaping[t.Index()] = escape
		}
	}

	// Restart iteration.
	t = t.Restart()

	tagLength := 2 * t.Len()
	idLen += tagLength + 1 // account for separators and brackets

	if needEscaping == nil {
		return quoteIDSimple(dst, idLen, t)
	}

	// Extend as required.
	dst = resizeOrGrow(dst, idLen)
	lastIndex := t.Len() - 1
	dst[0] = leftBracket
	idx := 1
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		idx = writeAtIndex(dst, tag.Name, tag.Value, needEscaping[t.Index()], idx)
		if t.Index() != lastIndex {
			dst[idx] = sep
			idx++
		}
	}
	dst[idx] = rightBracket

	// Take the length.
	dst = dst[:idx+1]

	return dst
}

func resizeOrGrow(dst []byte, l int) []byte {
	if cap(dst) < l {
		return make([]byte, l)
	}
	return dst[:l]
}

// quoteIDSimple returns a simple quote ID, note: must check it.Err() after if using iterator.
func quoteIDSimple(dst []byte, idLen int, t tagsIter) []byte {
	dst = resizeOrGrow(dst, idLen)
	dst[0] = leftBracket
	idx := 1
	lastIndex := t.Len() - 1
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		idx += copy(dst[idx:], tag.Name)
		dst[idx] = eq
		idx++
		idx = strconv.QuoteSimple(dst, tag.Value, idx)
		if t.Index() != lastIndex {
			dst[idx] = sep
			idx++
		}
	}
	dst[idx] = rightBracket

	return dst[:idx+1]
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

func prependMetaID(dst []byte, t tagsIter) []byte {
	t = t.Restart()
	length, metaLengths := prependMetaLen(t)

	// Restart for iteration.
	t = t.Restart()

	// Ensure has capacity
	dst = resizeOrGrow(dst, length)
	idx := writeTagLengthMeta(dst, metaLengths)
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		idx += copy(dst[idx:], tag.Name)
		idx += copy(dst[idx:], tag.Value)
	}

	return dst[:idx]
}

func writeTagLengthMeta(dst []byte, lengths []int) int {
	idx := writer.WriteIntegers(dst, lengths, sep, 0)
	dst[idx] = finish
	return idx + 1
}

func prependMetaLen(t tagsIter) (int, []int) {
	idLen := 1 // account for separator

	tagLengths := make([]int, t.Len()*2)
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		tagLen := len(tag.Name)
		tagLengths[2*t.Index()] = tagLen
		idLen += tagLen
		tagLen = len(tag.Value)
		tagLengths[2*t.Index()+1] = tagLen
		idLen += tagLen
	}

	prefixLen := writer.IntsLength(tagLengths)
	return idLen + prefixLen, tagLengths
}

func graphiteID(dst []byte, t tagsIter) []byte {
	t = t.Restart()
	length := graphiteIDLen(t)

	// Restart iteration.
	t = t.Restart()

	// Ensure has capacity
	dst = resizeOrGrow(dst, length)
	idx := 0
	lastIndex := t.Len() - 1
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		idx += copy(dst[idx:], tag.Value)
		if t.Index() != lastIndex {
			dst[idx] = graphiteSep
			idx++
		}
	}
	dst = dst[:idx]

	return dst
}

func graphiteIDLen(t tagsIter) int {
	idLen := t.Len() - 1 // account for separators
	for t, tag, next := t.Next(); next; t, tag, next = t.Next() {
		idLen += len(tag.Value)
	}

	return idLen
}
