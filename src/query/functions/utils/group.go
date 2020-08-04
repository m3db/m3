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
	"bytes"
	"sort"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

type group struct {
	buckets []int
	tags    models.Tags
}

type withKeysTags func(tags models.Tags, matchingTags [][]byte) models.Tags

func includeKeysTags(tags models.Tags, matchingTags [][]byte) models.Tags {
	return tags.TagsWithKeys(matchingTags)
}

func excludeKeysTags(tags models.Tags, matchingTags [][]byte) models.Tags {
	return tags.TagsWithoutKeys(matchingTags).WithoutName()
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
	var tagsFunc withKeysTags
	if without {
		tagsFunc = excludeKeysTags
	} else {
		tagsFunc = includeKeysTags
	}

	groups := make(map[uint64]*group)
	for i, meta := range metas {
		tags := tagsFunc(meta.Tags, matchingTags)
		// NB(arnikola): Get the ID of the series with relevant tags
		id := tags.HashedID()
		if val, ok := groups[id]; ok {
			// If ID has been seen, the corresponding grouped
			// series for this index already exists; add the
			// current index to the bucket for that
			// grouped series
			val.buckets = append(val.buckets, i)
		} else {
			// If ID has not been seen, create a grouped series
			// with the appropriate tags, and add the current index
			// to the bucket for that grouped series
			groups[id] = &group{
				buckets: []int{i},
				tags:    tags,
			}
		}
	}

	sortedGroups := sortGroups(groups)

	groupedBuckets := make([][]int, len(groups))
	groupedMetas := make([]block.SeriesMeta, len(groups))

	for i, group := range sortedGroups {
		groupedBuckets[i] = group.buckets
		groupedMetas[i] = block.SeriesMeta{
			Tags: group.tags,
			Name: opName,
		}
	}

	return groupedBuckets, groupedMetas
}

func sortGroups(groups map[uint64]*group) []*group {
	result := make([]*group, 0, len(groups))

	for _, group := range groups {
		result = append(result, group)
	}

	sort.Slice(result, func(i int, j int) bool {
		return compareTagSets(result[i].tags, result[j].tags) < 0
	})

	return result
}

func compareTagSets(a, b models.Tags) int {
	l := a.Len()

	if b.Len() < l {
		l = b.Len()
	}

	for i := 0; i < l; i++ {
		byName := bytes.Compare(a.Tags[i].Name, b.Tags[i].Name)
		if byName != 0 {
			return byName
		}
		byValue := bytes.Compare(a.Tags[i].Value, b.Tags[i].Value)
		if byValue != 0 {
			return byValue
		}
	}
	// If all tags so far were in common, the set with fewer tags comes first.
	return a.Len() - b.Len()
}
