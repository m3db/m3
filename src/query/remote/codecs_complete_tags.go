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
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xtime "github.com/m3db/m3/src/x/time"
)

func decodeTagNamesOnly(
	response *rpc.TagNames,
) []consolidators.CompletedTag {
	names := response.GetNames()
	tags := make([]consolidators.CompletedTag, len(names))
	for i, name := range names {
		tags[i] = consolidators.CompletedTag{Name: name}
	}

	return tags
}

func decodeTagProperties(
	response *rpc.TagValues,
) []consolidators.CompletedTag {
	values := response.GetValues()
	tags := make([]consolidators.CompletedTag, len(values))
	for i, value := range values {
		tags[i] = consolidators.CompletedTag{
			Name:   value.GetKey(),
			Values: value.GetValues(),
		}
	}

	return tags
}

func decodeCompleteTagsResponse(
	response *rpc.CompleteTagsResponse,
	completeNameOnly bool,
) ([]consolidators.CompletedTag, error) {
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
	options *storage.FetchOptions,
) (*rpc.CompleteTagsRequest, error) {
	completionType := rpc.CompleteTagsType_DEFAULT
	if query.CompleteNameOnly {
		completionType = rpc.CompleteTagsType_TAGNAME
	}

	matchers, err := encodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	opts, err := encodeFetchOptions(options)
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
			Start:          int64(query.Start),
			End:            int64(query.End),
			Options:        opts,
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
		Start:            xtime.UnixNano(opts.GetStart()),
		End:              xtime.UnixNano(opts.GetEnd()),
	}, nil
}

func encodeToCompressedCompleteTagsDefaultResult(
	results *consolidators.CompleteTagsResult,
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

		Meta: encodeResultMetadata(results.Metadata),
	}, nil
}

func encodeToCompressedCompleteTagsNameOnlyResult(
	results *consolidators.CompleteTagsResult,
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

		Meta: encodeResultMetadata(results.Metadata),
	}, nil
}

func encodeToCompressedCompleteTagsResult(
	results *consolidators.CompleteTagsResult,
) (*rpc.CompleteTagsResponse, error) {
	if results.CompleteNameOnly {
		return encodeToCompressedCompleteTagsNameOnlyResult(results)
	}

	return encodeToCompressedCompleteTagsDefaultResult(results)
}
