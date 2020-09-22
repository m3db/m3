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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/serialize"
)

func multiTagResultsToM3TagProperties(
	results []consolidators.MultiTagResult,
	encoderPool serialize.TagEncoderPool,
) (*rpc.M3TagProperties, error) {
	props := make([]rpc.M3TagProperty, len(results))
	for i, result := range results {
		tags, err := compressedTagsFromTagIterator(result.Iter, encoderPool)
		if err != nil {
			return nil, err
		}

		props[i] = rpc.M3TagProperty{
			Id:             result.ID.Bytes(),
			CompressedTags: tags,
		}
	}

	pprops := make([]*rpc.M3TagProperty, len(props))
	for i := range props {
		pprops[i] = &props[i]
	}

	return &rpc.M3TagProperties{
		Properties: pprops,
	}, nil
}

// encodeToCompressedSearchResult encodes SearchResults to a compressed
// search result.
func encodeToCompressedSearchResult(
	results []consolidators.MultiTagResult,
	metadata block.ResultMetadata,
	pools encoding.IteratorPools,
) (*rpc.SearchResponse, error) {
	if pools == nil {
		return nil, errors.ErrCannotEncodeCompressedTags
	}

	encoderPool := pools.TagEncoder()
	if encoderPool == nil {
		return nil, errors.ErrCannotEncodeCompressedTags
	}

	compressedTags, err := multiTagResultsToM3TagProperties(results, encoderPool)
	if err != nil {
		return nil, err
	}

	return &rpc.SearchResponse{
		Value: &rpc.SearchResponse_Compressed{
			Compressed: compressedTags,
		},

		Meta: encodeResultMetadata(metadata),
	}, nil
}

func decodeDecompressedSearchResponse(
	response *rpc.TagProperties,
	pools encoding.IteratorPools,
) (models.Metrics, error) {
	return nil, errors.ErrNotImplemented
}

func decodeCompressedSearchResponse(
	response *rpc.M3TagProperties,
	pools encoding.IteratorPools,
) ([]consolidators.MultiTagResult, error) {
	if pools == nil || pools.CheckedBytesWrapper() == nil || pools.TagDecoder() == nil {
		return nil, errors.ErrCannotDecodeCompressedTags
	}

	cbwPool := pools.CheckedBytesWrapper()
	decoderPool := pools.TagDecoder()
	idPool := pools.ID()

	props := response.GetProperties()
	decoded := make([]consolidators.MultiTagResult, len(props))
	for i, prop := range props {
		checkedBytes := cbwPool.Get(prop.GetCompressedTags())
		decoder := decoderPool.Get()
		decoder.Reset(checkedBytes)
		if err := decoder.Err(); err != nil {
			return nil, err
		}

		id := idPool.BinaryID(cbwPool.Get(prop.GetId()))
		decoded[i] = consolidators.MultiTagResult{
			ID: id,
			// Copy underlying TagIterator bytes before closing the decoder and returning it to the pool
			Iter: decoder.Duplicate(),
		}

		decoder.Close()
	}

	return decoded, nil
}

func decodeSearchResponse(
	response *rpc.SearchResponse,
	pools encoding.IteratorPools,
	tagOptions models.TagOptions,
) (models.Metrics, error) {
	if compressed := response.GetCompressed(); compressed != nil {
		results, err := decodeCompressedSearchResponse(compressed, pools)
		if err != nil {
			return nil, err
		}

		metrics := make(models.Metrics, len(results))
		for i, r := range results {
			m, err := storage.FromM3IdentToMetric(r.ID, r.Iter, tagOptions)
			if err != nil {
				return nil, err
			}

			metrics[i] = m
		}

		return metrics, nil
	}

	if decompressed := response.GetDecompressed(); decompressed != nil {
		return decodeDecompressedSearchResponse(decompressed, pools)
	}

	return nil, errors.ErrUnexpectedGRPCResponseType
}

// encodeSearchRequest encodes search request into rpc SearchRequest
func encodeSearchRequest(
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*rpc.SearchRequest, error) {
	matchers, err := encodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	opts, err := encodeFetchOptions(options)
	if err != nil {
		return nil, err
	}

	return &rpc.SearchRequest{
		Matchers: &rpc.SearchRequest_TagMatchers{
			TagMatchers: matchers,
		},

		Start:   fromTime(query.Start),
		End:     fromTime(query.End),
		Options: opts,
	}, nil
}

// decodeSearchRequest decodes rpc search request to read query and read options
func decodeSearchRequest(
	req *rpc.SearchRequest,
) (*storage.FetchQuery, error) {
	matchers, err := decodeTagMatchers(req.GetTagMatchers())
	if err != nil {
		return nil, err
	}

	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       toTime(req.GetStart()),
		End:         toTime(req.GetEnd()),
	}, nil
}
