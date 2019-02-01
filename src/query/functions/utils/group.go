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

package utils

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

type withKeysID func(tags models.Tags, matchingTags [][]byte) uint64

func includeKeysID(tags models.Tags, matchingTags [][]byte) uint64 {
	return tags.TagsWithKeys(matchingTags).HashedID()
}

func excludeKeysID(tags models.Tags, matchingTags [][]byte) uint64 {
	return tags.TagsWithoutKeys(matchingTags).HashedID()
}

type withKeysTags func(tags models.Tags, matchingTags [][]byte) models.Tags

func includeKeysTags(tags models.Tags, matchingTags [][]byte) models.Tags {
	return tags.TagsWithKeys(matchingTags)
}

func excludeKeysTags(tags models.Tags, matchingTags [][]byte) models.Tags {
	return tags.TagsWithoutKeys(matchingTags)
}

// GroupSeries groups series by tags.
// It gives a list of seriesMeta for the grouped series,
// and a list of corresponding buckets which signify which
// series are mapped to which grouped outputs
func GroupSeries(
	matchingTags [][]byte,
	without bool,
	opName []byte,
	metas []block.SeriesMeta,
) ([][]int, []block.SeriesMeta) {
	var idFunc withKeysID
	var tagsFunc withKeysTags
	if without {
		idFunc = excludeKeysID
		tagsFunc = excludeKeysTags
	} else {
		idFunc = includeKeysID
		tagsFunc = includeKeysTags
	}

	type tagMatch struct {
		buckets []int
		tags    models.Tags
	}

	tagMap := make(map[uint64]*tagMatch)
	for i, meta := range metas {
		// NB(arnikola): Get the ID of the series with relevant tags
		id := idFunc(meta.Tags, matchingTags)
		if val, ok := tagMap[id]; ok {
			// If ID has been seen, the corresponding grouped
			// series for this index already exists; add the
			// current index to the bucket for that
			// grouped series
			val.buckets = append(val.buckets, i)
		} else {
			// If ID has not been seen, create a grouped series
			// with the appropriate tags, and add the current index
			// to the bucket for that grouped series
			tagMap[id] = &tagMatch{
				buckets: []int{i},
				tags:    tagsFunc(meta.Tags, matchingTags),
			}
		}
	}

	groupedBuckets := make([][]int, len(tagMap))
	groupedMetas := make([]block.SeriesMeta, len(tagMap))
	i := 0
	for _, v := range tagMap {
		groupedBuckets[i] = v.buckets
		groupedMetas[i] = block.SeriesMeta{
			Tags: v.tags,
			Name: opName,
		}
		i++
	}

	return groupedBuckets, groupedMetas
}
