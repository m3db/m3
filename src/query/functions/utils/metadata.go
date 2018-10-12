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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

// FlattenMetadata applies all shared tags from Metadata to each SeriesMeta
func FlattenMetadata(
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
) []block.SeriesMeta {
	for i, metas := range seriesMeta {
		seriesMeta[i].Tags = metas.Tags.Add(meta.Tags)
	}

	return seriesMeta
}

// DedupeMetadata applies all shared tags from Metadata to each SeriesMeta
func DedupeMetadata(
	seriesMeta []block.SeriesMeta,
) (models.Tags, []block.SeriesMeta) {
	if len(seriesMeta) == 0 {
		return models.EmptyTags(), seriesMeta
	}

	commonKeys := make([][]byte, 0, seriesMeta[0].Tags.Len())
	commonTags := make(map[string][]byte, seriesMeta[0].Tags.Len())
	// For each tag in the first series, read through list of seriesMetas;
	// if key not found or value differs, this is not a shared tag
	var distinct bool
	for _, t := range seriesMeta[0].Tags.Tags {
		distinct = false
		for _, metas := range seriesMeta[1:] {
			if val, ok := metas.Tags.Get(t.Name); ok {
				if !bytes.Equal(val, t.Value) {
					distinct = true
					break
				}
			} else {
				distinct = true
				break
			}
		}

		if !distinct {
			// This is a shared tag; add it to shared meta
			commonKeys = append(commonKeys, t.Name)
			commonTags[string(t.Name)] = t.Value
		}
	}

	for i, meta := range seriesMeta {
		seriesMeta[i].Tags = meta.Tags.TagsWithoutKeys(commonKeys)
	}

	tags := models.NewTags(len(commonTags), seriesMeta[0].Tags.Opts)
	for n, v := range commonTags {
		tags = tags.AddTag(models.Tag{Name: []byte(n), Value: v})
	}

	return tags, seriesMeta
}
