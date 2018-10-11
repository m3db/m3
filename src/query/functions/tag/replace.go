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
	"fmt"
	"regexp"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

// TagReplaceType creates a new tag for all metrics containing a tag
// with the given name whose value matches the given regex.
// NB: This does not actually remove the original tag, but will override
// the existing tag if the source and destination name parameters are equal.
const TagReplaceType = "label_replace"

// Builds and adds the replaced tag if the source tag is found in the list,
// and if the value of that tag matches the given regex. Returns false if the series
// does not contain the source tag name, regardless of if the value matches the regex.
func addTagIfFoundAndValid(
	tags models.Tags,
	val []byte,
	destinationName []byte,
	destinationValRegex []byte,
	regex *regexp.Regexp,
) (models.Tag, bool) {
	indices := regex.FindSubmatchIndex(val)
	if indices == nil {
		return models.Tag{}, false
	}

	destinationVal := regex.Expand([]byte{}, destinationValRegex, val, indices)
	return models.Tag{Name: destinationName, Value: destinationVal}, true
}

func makeTagReplaceFunc(params []string) (tagTransformFunc, error) {
	if len(params) != 4 {
		return nil, fmt.Errorf("invalid number of args for tag replace: %d", len(params))
	}

	regex, err := regexp.Compile(params[3])
	if err != nil {
		return nil, err
	}

	destinationName := []byte(params[0])
	destinationValRegex := []byte(params[1])
	sourceName := []byte(params[2])

	return func(meta *block.Metadata, seriesMeta []block.SeriesMeta) {
		// Optimization if all joining series are shared by the block,
		// or if there is only a shared metadata and no single series metas.
		val, found := meta.Tags.Get(sourceName)
		if found {
			tag, valid := addTagIfFoundAndValid(
				meta.Tags,
				val,
				destinationName,
				destinationValRegex,
				regex,
			)

			// NB: If the tag exists in shared block tag list, it cannot also exist
			// in the tag lists for the series metadatas, so it's valid to short
			// circuit here.
			if valid {
				meta.Tags = meta.Tags.AddOrUpdateTag(tag)
			}

			return
		}

		if len(seriesMeta) == 0 {
			return
		}

		for i, meta := range seriesMeta {
			val, found := meta.Tags.Get(sourceName)
			if !found {
				continue
			}

			if tag, valid := addTagIfFoundAndValid(
				meta.Tags,
				val,
				destinationName,
				destinationValRegex,
				regex,
			); valid {
				seriesMeta[i].Tags = seriesMeta[i].Tags.AddOrUpdateTag(tag)
			}
		}
	}, nil
}
