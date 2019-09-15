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

package binary

import (
	"bytes"
	"errors"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

var (
	errRExhausted = errors.New("right iter exhausted while left iter has values")
	errLExhausted = errors.New("left iter exhausted while right iter has values")
)

type indexMatcher struct {
	lhsIndex int
	rhsIndex int
}

// hashFunc returns a function that calculates the signature for a metric
// ignoring the provided labels. If on, then only the given labels are used.
func hashFunc(on bool, names ...[]byte) func(models.Tags) uint64 {
	if on {
		return func(tags models.Tags) uint64 {
			return tags.TagsWithKeys(names).HashedID()
		}
	}

	return func(tags models.Tags) uint64 {
		return tags.TagsWithoutKeys(names).HashedID()
	}
}

const initIndexSliceLength = 10

var (
	errMismatchedBounds     = errors.New("block bounds are mismatched")
	errMismatchedStepCounts = errors.New("block step counts are mismatched")
	errLeftScalar           = errors.New("expected left scalar but node type" +
		" incorrect")
	errRightScalar = errors.New("expected right scalar but node" +
		" type incorrect")
	errNoModifierForComparison = errors.New("comparisons between scalars must" +
		" use BOOL modifier")
	errNoMatching = errors.New("vector matching parameters must" +
		" be provided for binary operations between series")
)

func tagMap(t models.Tags) map[string]models.Tag {
	m := make(map[string]models.Tag, t.Len())
	for _, tag := range t.Tags {
		m[string(tag.Name)] = tag
	}

	return m
}

// Iff one of left or right is a time block, match match one to many
// against it, and match everything.
func defaultVectorMatcherBuilder(lhs, rhs block.Block) VectorMatching {
	left := lhs.Info().BaseType() == block.BlockTime
	right := rhs.Info().BaseType() == block.BlockTime

	if left {
		if right {
			return VectorMatching{
				Set:  true,
				Card: CardOneToOne,
			}
		}

		return VectorMatching{
			Set:  true,
			Card: CardOneToMany,
			On:   true,
		}
	}

	if right {
		return VectorMatching{
			Set:  true,
			Card: CardManyToOne,
			On:   true,
		}
	}

	return VectorMatching{Set: false}
}

func combineMetaAndSeriesMeta(
	meta, otherMeta block.Metadata,
	seriesMeta, otherSeriesMeta []block.SeriesMeta,
) (block.Metadata, []block.SeriesMeta, []block.SeriesMeta, error) {
	if !meta.Bounds.Equals(otherMeta.Bounds) {
		return block.Metadata{},
			[]block.SeriesMeta{},
			[]block.SeriesMeta{},
			errMismatchedBounds
	}

	// NB (arnikola): mutating tags in `meta` to avoid allocations
	leftTags := meta.Tags
	otherTags := tagMap(otherMeta.Tags)

	metaTagsToAdd := models.NewTags(leftTags.Len(), leftTags.Opts)
	otherMetaTagsToAdd := models.NewTags(len(otherTags), leftTags.Opts)
	tags := models.NewTags(leftTags.Len(), leftTags.Opts)

	for _, t := range leftTags.Tags {
		name := string(t.Name)
		if otherTag, ok := otherTags[name]; ok {
			if bytes.Equal(t.Value, otherTag.Value) {
				tags = tags.AddTag(t)
			} else {
				// If both metas have the same common tag  with different
				// values explicitly add it to each seriesMeta.
				metaTagsToAdd = metaTagsToAdd.AddTag(t)
				otherMetaTagsToAdd = otherMetaTagsToAdd.AddTag(otherTag)
			}

			// NB(arnikola): delete common tag from otherTags as it
			// has already been handled
			delete(otherTags, name)
		} else {
			// Tag does not exist on otherMeta explicitly add it to each seriesMeta
			metaTagsToAdd = metaTagsToAdd.AddTag(t)
		}
	}

	// Iterate over otherMeta common tags and explicitly add
	// remaining tags to otherSeriesMeta
	for _, otherTag := range otherTags {
		otherMetaTagsToAdd = otherMetaTagsToAdd.AddTag(otherTag)
	}

	// Set common tags
	meta.Tags = tags
	for i, m := range seriesMeta {
		seriesMeta[i].Tags = m.Tags.Add(metaTagsToAdd)
	}

	for i, m := range otherSeriesMeta {
		otherSeriesMeta[i].Tags = m.Tags.Add(otherMetaTagsToAdd)
	}

	meta.ResultMetadata = meta.ResultMetadata.
		CombineMetadata(otherMeta.ResultMetadata)

	return meta,
		seriesMeta,
		otherSeriesMeta,
		nil
}

// NB: binary functions should remove the name tag from relevant series.
func removeNameTags(
	meta block.Metadata,
	metas []block.SeriesMeta,
) (block.Metadata, []block.SeriesMeta) {
	meta.Tags = meta.Tags.WithoutName()
	for i, sm := range metas {
		metas[i].Tags = sm.Tags.WithoutName()
	}

	return meta, metas
}
