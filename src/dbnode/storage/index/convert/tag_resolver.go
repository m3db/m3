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

package convert

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
)

var (
	// EmptyTagMetadataResolver is empty tags iter metadata resolver.
	EmptyTagMetadataResolver = NewTagsIterMetadataResolver(ident.EmptyTagIterator)

	// ErrUnknownTagMetadataResolverType is unknown tag metadata resolver type error.
	ErrUnknownTagMetadataResolverType = errors.New("unknown tag metadata resolver type")
)

type tagResolverType uint8

const (
	tagResolverEncodedTags tagResolverType = iota
	tagResolverIter
	tagResolverTags
)

// TagMetadataResolver represents metadata resolver.
type TagMetadataResolver struct {
	resolverType tagResolverType
	encodedTags  ts.EncodedTags
	tagsIter     ident.TagIterator
	tags         ident.Tags
}

// NewEncodedTagsMetadataResolver returns metadata resolver which accepts encoded tags.
func NewEncodedTagsMetadataResolver(encodedTags ts.EncodedTags) TagMetadataResolver {
	return TagMetadataResolver{
		resolverType: tagResolverEncodedTags,
		encodedTags:  encodedTags,
	}
}

// NewTagsIterMetadataResolver returns metadata resolver which accepts tags iterator.
func NewTagsIterMetadataResolver(tagsIter ident.TagIterator) TagMetadataResolver {
	return TagMetadataResolver{
		resolverType: tagResolverIter,
		tagsIter:     tagsIter,
	}
}

// NewTagsMetadataResolver returns metadata resolver which accepts tags.
func NewTagsMetadataResolver(tags ident.Tags) TagMetadataResolver {
	return TagMetadataResolver{
		resolverType: tagResolverTags,
		tags:         tags,
	}
}

// Resolve resolves doc.Metadata from seriesID.
func (t TagMetadataResolver) Resolve(id ident.ID) (doc.Metadata, error) {
	switch t.resolverType {
	case tagResolverEncodedTags:
		return FromSeriesIDAndEncodedTags(id.Bytes(), t.encodedTags)
	case tagResolverIter:
		// NB(r): Rewind so we record the tag iterator from the beginning.
		tagsIter := t.tagsIter.Duplicate()

		seriesMetadata, err := FromSeriesIDAndTagIter(id, tagsIter)
		tagsIter.Close()
		if err != nil {
			return doc.Metadata{}, err
		}
		return seriesMetadata, nil
	case tagResolverTags:
		return FromSeriesIDAndTags(id, t.tags)
	default:
		return doc.Metadata{}, ErrUnknownTagMetadataResolverType
	}
}
