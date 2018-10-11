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

package tag

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
)

// TagJoinType joins the values of given tags using a given separator
// and adds them to a given destination tag. It can combine any number of tags.
// NB: This will override an existing tag if the given tag exists in the tag list.
const TagJoinType = "label_join"

func combineTagsWithSeparator(name []byte, separator []byte, values [][]byte) models.Tag {
	l := len(values)
	if l == 1 {
		return models.Tag{Name: name, Value: values[0]}
	}

	b := bytes.Join(values, separator)
	return models.Tag{Name: name, Value: b}
}

// Gets tag values from models.
// NB: duplicate tag names giving duplicate values is valid.
func tagsInOrder(names [][]byte, tags models.Tags) [][]byte {
	orderedTags := make([][]byte, 0, len(names))
	for _, name := range names {
		for _, tag := range tags {
			if bytes.Equal(name, tag.Name) {
				orderedTags = append(orderedTags, tag.Value)
			}
		}
	}

	return orderedTags
}

// ident function.
func identFunc(
	m block.Metadata,
	sm []block.SeriesMeta,
) (block.Metadata, []block.SeriesMeta) {
	return m, sm
}

func makeTagJoinFunc(params []string) (tagTransformFunc, error) {
	if len(params) < 2 {
		return nil, fmt.Errorf("invalid number of args for tag join: %d", len(params))
	}

	// return shortcircuting noop function if no joining names are provided.
	if len(params) == 3 {
		return identFunc, nil
	}

	name := []byte(params[0])
	sep := []byte(params[1])
	tagNames := make([][]byte, len(params)-2)
	uniqueTagCount := utils.UniqueCount(params[2:])
	for i, tag := range params[2:] {
		tagNames[i] = ([]byte(tag))
	}

	return func(
		meta block.Metadata,
		seriesMeta []block.SeriesMeta,
	) (block.Metadata, []block.SeriesMeta) {
		matchingCommonTags := meta.Tags.TagsWithKeys(tagNames)
		lMatching := len(matchingCommonTags)
		// Optimization if all joining series are shared by the block,
		// or if there is only a shared metadata and no single series metas.
		if lMatching == uniqueTagCount || len(seriesMeta) == 0 {
			if lMatching > 0 {
				ordered := tagsInOrder(tagNames, matchingCommonTags)
				meta.Tags = meta.Tags.AddOrUpdateTag(combineTagsWithSeparator(name, sep, ordered))
			}

			return meta, seriesMeta
		}

		for i, meta := range seriesMeta {
			seriesTags := meta.Tags.TagsWithKeys(tagNames)
			seriesTags = seriesTags.Add(matchingCommonTags)
			if len(seriesTags) > 0 {
				ordered := tagsInOrder(tagNames, seriesTags)
				seriesMeta[i].Tags = seriesMeta[i].Tags.
					AddOrUpdateTag(combineTagsWithSeparator(name, sep, ordered))
			}
		}

		return meta, seriesMeta
	}, nil
}
