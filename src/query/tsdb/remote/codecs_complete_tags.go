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

package remote

import (
	"github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage"
)

func decodeTagNamesOnly(
	response *rpc.TagNames,
) *storage.CompleteTagsResult {
	names := response.GetNames()
	tags := make([]storage.CompletedTag, len(names))
	for i, name := range names {
		tags[i] = storage.CompletedTag{Name: name}
	}

	return &storage.CompleteTagsResult{
		CompleteNameOnly: true,
		CompletedTags:    tags,
	}
}

func decodeTagProperties(
	response *rpc.TagProperties,
) *storage.CompleteTagsResult {
	props := response.GetProperties()
	tags := make([]storage.CompletedTag, len(props))
	for i, prop := range props {
		tags[i] = storage.CompletedTag{
			Name:   prop.GetKey(),
			Values: prop.GetValues(),
		}
	}

	return &storage.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags:    tags,
	}
}

func decodeCompleteTagsResponse(
	response *rpc.CompleteTagsResponse,
) (*storage.CompleteTagsResult, error) {
	if names := response.GetNamesOnly(); names != nil {
		return decodeTagNamesOnly(names), nil
	}

	if props := response.GetDefault(); props != nil {
		return decodeTagProperties(props), nil
	}

	return nil, errors.ErrUnexpectedGRPCResponseType
}

func encodeCompleteTagsRequest(
	query *storage.CompleteTagsQuery,
) (*rpc.CompleteTagsRequest, error) {
	searchType := rpc.SearchType_DEFAULT
	if query.CompleteNameOnly {
		searchType = rpc.SearchType_TAGNAME
	}

	return &rpc.CompleteTagsRequest{
		Query: query.Query,
		Type:  searchType,
		Options: &rpc.SearchRequestOptions{
			FilterNameTags: query.FilterNameTags,
		},
	}, nil
}
