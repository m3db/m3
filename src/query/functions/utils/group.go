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

type withKeysID func(tags models.Tags, matching []string) uint64

func includeKeysID(tags models.Tags, matching []string) uint64 {
	return tags.IDWithKeys(matching...)
}

func excludeKeysID(tags models.Tags, matching []string) uint64 {
	return tags.IDWithExcludes(matching...)
}

type withKeysTags func(tags models.Tags, matching []string) models.Tags

func includeKeysTags(tags models.Tags, matching []string) models.Tags {
	return tags.TagsWithKeys(matching)
}

func excludeKeysTags(tags models.Tags, matching []string) models.Tags {
	return tags.TagsWithoutKeys(matching)
}

// GroupSeries groups series by tags.
// It gives a list of seriesMeta for the grouped series,
// and a list of corresponding indices which signify which
// series are mapped to which grouped outputs
func GroupSeries(
	matching []string,
	without bool,
	opName string,
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
		indices []int
		tags    models.Tags
	}

	tagMap := make(map[uint64]*tagMatch)
	for i, meta := range metas {
		id := idFunc(meta.Tags, matching)
		if val, ok := tagMap[id]; ok {
			val.indices = append(val.indices, i)
		} else {
			tagMap[id] = &tagMatch{
				indices: []int{i},
				tags:    tagsFunc(meta.Tags, matching),
			}
		}
	}

	groupedIndices := make([][]int, len(tagMap))
	groupedMetas := make([]block.SeriesMeta, len(tagMap))
	i := 0
	for _, v := range tagMap {
		groupedIndices[i] = v.indices
		groupedMetas[i] = block.SeriesMeta{
			Tags: v.tags,
			Name: opName,
		}
		i++
	}

	return groupedIndices, groupedMetas
}
