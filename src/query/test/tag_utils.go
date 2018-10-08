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

package test

import (
	"github.com/m3db/m3/src/query/models"
)

// StringTagsToTags converts string tags to tags.
func StringTagsToTags(s StringTags) models.Tags {
	tags := models.NewTags(len(s), models.NewTagOptions())
	for _, t := range s {
		tags.AddTag(models.Tag{Name: []byte(t.N), Value: []byte(t.V)})
	}

	return tags
}

// StringTagsSliceToTagSlice converts a slice of string tags to a slice of tags.
func StringTagsSliceToTagSlice(s []StringTags) []models.Tags {
	tags := make([]models.Tags, len(s))

	for i, stringTags := range s {
		tags[i] = StringTagsToTags(stringTags)
	}

	return tags
}

// StringTags is a slice of string tags.
type StringTags []StringTag

// StringTag is a tag containing string key value pairs.
type StringTag struct {
	N, V string
}

// TagSliceToTags converts a slice of tags to tags.
func TagSliceToTags(s []models.Tag) models.Tags {
	tags := models.EmptyTags()
	for _, t := range s {
		tags.AddTag(t)
	}

	return tags
}

// TagSliceSliceToTagSlice converts a slice of tag slices to a slice of tags.
func TagSliceSliceToTagSlice(s [][]models.Tag) []models.Tags {
	tags := make([]models.Tags, len(s))

	for i, t := range s {
		tags[i] = TagSliceToTags(t)
	}

	return tags
}
