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

// FlattenMetadata applies all shared tags from Metadata to each SeriesMeta
func FlattenMetadata(
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
) []block.SeriesMeta {
	for k, v := range meta.Tags {
		for _, metas := range seriesMeta {
			metas.Tags[k] = v
		}
	}

	return seriesMeta
}

// DedupeMetadata applies all shared tags from Metadata to each SeriesMeta
func DedupeMetadata(
	seriesMeta []block.SeriesMeta,
) (models.Tags, []block.SeriesMeta) {
	tags := make(models.Tags)
	if len(seriesMeta) == 0 {
		return tags, seriesMeta
	}

	// For each tag in the first series, read through list of seriesMetas;
	// if key not found or value differs, this is not a shared tag
	var distinct bool
	for k, v := range seriesMeta[0].Tags {
		distinct = false
		for _, metas := range seriesMeta[1:] {
			if val, ok := metas.Tags[k]; ok {
				if val != v {
					distinct = true
					break
				}
			} else {
				distinct = true
				break
			}
		}

		if !distinct {
			// This is a shared tag; add it to shared meta and remove
			// it from each seriesMeta
			tags[k] = v
			for _, metas := range seriesMeta {
				delete(metas.Tags, k)
			}
		}
	}

	return tags, seriesMeta
}
