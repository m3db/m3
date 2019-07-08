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
) []storage.CompletedTag {
	names := response.GetNames()
	tags := make([]storage.CompletedTag, len(names))
	for i, name := range names {
		tags[i] = storage.CompletedTag{Name: name}
	}

	return tags
}

func decodeTagProperties(
	response *rpc.TagValues,
) []storage.CompletedTag {
	values := response.GetValues()
	tags := make([]storage.CompletedTag, len(values))
	for i, value := range values {
		tags[i] = storage.CompletedTag{
			Name:   value.GetKey(),
			Values: value.GetValues(),
		}
	}

	return tags
}

func decodeCompleteTagsResponse(
	response *rpc.CompleteTagsResponse,
	completeNameOnly bool,
) ([]storage.CompletedTag, error) {
	if names := response.GetNamesOnly(); names != nil {
		if !completeNameOnly {
			return nil, errors.ErrInconsistentCompleteTagsType
		}

		return decodeTagNamesOnly(names), nil
	}

	if props := response.GetDefault(); props != nil {
		if completeNameOnly {
			return nil, errors.ErrInconsistentCompleteTagsType
		}

		return decodeTagProperties(props), nil
	}

	return nil, errors.ErrUnexpectedGRPCResponseType
}

func encodeCompleteTagsRequest(
	query *storage.CompleteTagsQuery,
) (*rpc.CompleteTagsRequest, error) {
	completionType := rpc.CompleteTagsType_DEFAULT
	if query.CompleteNameOnly {
		completionType = rpc.CompleteTagsType_TAGNAME
	}

	matchers, err := encodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	return &rpc.CompleteTagsRequest{
		Matchers: &rpc.CompleteTagsRequest_TagMatchers{
			TagMatchers: matchers,
		},
		Options: &rpc.CompleteTagsRequestOptions{
			Type:           completionType,
			FilterNameTags: query.FilterNameTags,
			Start:          fromTime(query.Start),
			End:            fromTime(query.End),
		},
	}, nil
}

func decodeCompleteTagsRequest(
	request *rpc.CompleteTagsRequest,
) (*storage.CompleteTagsQuery, error) {
	var (
		opts     = request.GetOptions()
		matchers = request.GetTagMatchers()
	)

	completeNameOnly := opts.GetType() == rpc.CompleteTagsType_TAGNAME
	tagMatchers, err := decodeTagMatchers(matchers)
	if err != nil {
		return nil, err
	}

	return &storage.CompleteTagsQuery{
		CompleteNameOnly: completeNameOnly,
		FilterNameTags:   opts.GetFilterNameTags(),
		TagMatchers:      tagMatchers,
		Start:            toTime(opts.GetStart()),
		End:              toTime(opts.GetEnd()),
	}, nil
}

func encodeToCompressedCompleteTagsDefaultResult(
	results *storage.CompleteTagsResult,
) (*rpc.CompleteTagsResponse, error) {
	tags := results.CompletedTags
	values := make([]*rpc.TagValue, 0, len(tags))
	for _, tag := range tags {
		values = append(values, &rpc.TagValue{
			Key:    tag.Name,
			Values: tag.Values,
		})
	}

	return &rpc.CompleteTagsResponse{
		Value: &rpc.CompleteTagsResponse_Default{
			Default: &rpc.TagValues{
				Values: values,
			},
		},
	}, nil
}

func encodeToCompressedCompleteTagsNameOnlyResult(
	results *storage.CompleteTagsResult,
) (*rpc.CompleteTagsResponse, error) {
	tags := results.CompletedTags
	names := make([][]byte, 0, len(tags))
	for _, tag := range tags {
		names = append(names, tag.Name)
	}

	return &rpc.CompleteTagsResponse{
		Value: &rpc.CompleteTagsResponse_NamesOnly{
			NamesOnly: &rpc.TagNames{
				Names: names,
			},
		},
	}, nil
}

func encodeToCompressedCompleteTagsResult(
	results *storage.CompleteTagsResult,
) (*rpc.CompleteTagsResponse, error) {
	if results.CompleteNameOnly {
		return encodeToCompressedCompleteTagsNameOnlyResult(results)
	}

	return encodeToCompressedCompleteTagsDefaultResult(results)
}
