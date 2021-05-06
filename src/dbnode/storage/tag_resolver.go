// Copyright (c) 2021 Uber Technologies, Inc.
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

package storage

import (
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
)

// EmptyTagMetadataResolver is empty tags iter metadata resolver.
var EmptyTagMetadataResolver = NewTagsIterMetadataResolver(ident.EmptyTagIterator)

type encodedTagsMetadataResolver struct {
	encodedTags ts.EncodedTags
}

func (e encodedTagsMetadataResolver) Resolve(id ident.ID) (doc.Metadata, error) {
	return convert.FromSeriesIDAndEncodedTags(id, e.encodedTags)
}

// NewEncodedTagsMetadataResolver returns metadata resolver which accepts encoded tags.
func NewEncodedTagsMetadataResolver(encodedTags ts.EncodedTags) ident.TagMetadataResolver {
	return encodedTagsMetadataResolver{
		encodedTags: encodedTags,
	}
}

type tagsIterMetadataResolver struct {
	tagsIter ident.TagIterator
}

func (t tagsIterMetadataResolver) Resolve(id ident.ID) (doc.Metadata, error) {
	// NB(r): Rewind so we record the tag iterator from the beginning.
	tagsIter := t.tagsIter.Duplicate()

	seriesMetadata, err := convert.FromSeriesIDAndTagIter(id, tagsIter)
	tagsIter.Close()
	if err != nil {
		return doc.Metadata{}, err
	}
	return seriesMetadata, nil
}

// NewTagsIterMetadataResolver returns metadata resolver which accepts tags iterator.
func NewTagsIterMetadataResolver(tagsIter ident.TagIterator) ident.TagMetadataResolver {
	return tagsIterMetadataResolver{
		tagsIter: tagsIter,
	}
}

type tagsMetadataResolver struct {
	tags ident.Tags
}

// NewTagsMetadataResolver returns metadata resolver which accepts tags.
func NewTagsMetadataResolver(tags ident.Tags) ident.TagMetadataResolver {
	return tagsMetadataResolver{
		tags: tags,
	}
}

func (t tagsMetadataResolver) Resolve(id ident.ID) (doc.Metadata, error) {
	return convert.FromSeriesIDAndTags(id, t.tags)
}
