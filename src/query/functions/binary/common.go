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

// VectorMatchCardinality describes the cardinality relationship
// of two Vectors in a binary operation.
type VectorMatchCardinality int

const (
	// CardOneToOne is used for one-one relationship
	CardOneToOne VectorMatchCardinality = iota
	// CardManyToOne is used for many-one relationship
	CardManyToOne
	// CardOneToMany is used for one-many relationship
	CardOneToMany
	// CardManyToMany is used for many-many relationship
	CardManyToMany
)

// VectorMatching describes how elements from two Vectors in a binary
// operation are supposed to be matched.
type VectorMatching struct {
	// The cardinality of the two Vectors.
	Card VectorMatchCardinality
	// MatchingLabels contains the labels which define equality of a pair of
	// elements from the Vectors.
	MatchingLabels [][]byte
	// On includes the given label names from matching,
	// rather than excluding them.
	On bool
	// Include contains additional labels that should be included in
	// the result from the side with the lower cardinality.
	Include []string
}

// HashFunc returns a function that calculates the signature for a metric
// ignoring the provided labels. If on, then the given labels are only used instead.
func HashFunc(on bool, names ...[]byte) func(models.Tags) uint64 {
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
	errMismatchedBounds        = errors.New("block bounds are mismatched")
	errMismatchedStepCounts    = errors.New("block step counts are mismatched")
	errLeftScalar              = errors.New("expected left scalar but node type incorrect")
	errRightScalar             = errors.New("expected right scalar but node type incorrect")
	errNoModifierForComparison = errors.New("comparisons between scalars must use BOOL modifier")
	errNoMatching              = errors.New("vector matching parameters must be provided for binary operations between series")
)

func tagMap(t models.Tags) map[string]models.Tag {
	m := make(map[string]models.Tag, t.Len())
	for _, tag := range t.Tags {
		m[string(tag.Name)] = tag
	}

	return m
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

	return meta,
		seriesMeta,
		otherSeriesMeta,
		nil
}

func appendValuesAtIndices(idxArray []int, iter block.StepIter, builder block.Builder) error {
	for index := 0; iter.Next(); index++ {
		step, err := iter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		for _, idx := range idxArray {
			builder.AppendValue(index, values[idx])
		}
	}

	return nil
}
