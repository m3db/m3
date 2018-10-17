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
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"

	"google.golang.org/grpc/metadata"
)

const reqIDKey = "reqid"

func fromTime(t time.Time) int64 {
	return storage.TimeToTimestamp(t)
}

func toTime(t int64) time.Time {
	return storage.TimestampToTime(t)
}

func encodeTags(tags models.Tags) []*rpc.Tag {
	encodedTags := make([]*rpc.Tag, 0, tags.Len())
	for _, t := range tags.Tags {
		encodedTags = append(encodedTags, &rpc.Tag{
			Name:  t.Name,
			Value: t.Value,
		})
	}

	return encodedTags
}

// EncodeFetchResult encodes fetch result to rpc response
func EncodeFetchResult(results *storage.FetchResult) *rpc.FetchResponse {
	series := make([]*rpc.Series, len(results.SeriesList))
	for i, result := range results.SeriesList {
		vLen := result.Len()
		datapoints := make([]*rpc.Datapoint, vLen)
		for j := 0; j < vLen; j++ {
			dp := result.Values().DatapointAt(j)
			datapoints[j] = &rpc.Datapoint{
				Timestamp: fromTime(dp.Timestamp),
				Value:     dp.Value,
			}
		}

		series[i] = &rpc.Series{
			Meta: &rpc.SeriesMetadata{
				Id: []byte(result.Name()),
			},
			Value: &rpc.Series_Decompressed{
				Decompressed: &rpc.DecompressedSeries{
					Datapoints: datapoints,
					Tags:       encodeTags(result.Tags),
				},
			},
		}
	}

	return &rpc.FetchResponse{
		Series: series,
	}
}

// DecodeDecompressedFetchResult decodes fetch results from a GRPC-compatible type.
func DecodeDecompressedFetchResult(
	name string,
	tagOptions models.TagOptions,
	rpcSeries []*rpc.DecompressedSeries,
) ([]*ts.Series, error) {
	tsSeries := make([]*ts.Series, len(rpcSeries))
	var err error
	for i, series := range rpcSeries {
		tsSeries[i], err = decodeTs(name, tagOptions, series)
		if err != nil {
			return nil, err
		}
	}

	return tsSeries, nil
}

func decodeTags(
	tags []*rpc.Tag,
	tagOptions models.TagOptions,
) models.Tags {
	modelTags := models.NewTags(len(tags), tagOptions)
	for _, t := range tags {
		modelTags = modelTags.AddTag(models.Tag{Name: t.GetName(), Value: t.GetValue()})
	}

	return modelTags
}

func decodeTs(
	name string,
	tagOptions models.TagOptions,
	r *rpc.DecompressedSeries,
) (*ts.Series, error) {
	values := decodeRawTs(r)
	tags := decodeTags(r.GetTags(), tagOptions)
	series := ts.NewSeries(name, values, tags)
	return series, nil
}

func decodeRawTs(r *rpc.DecompressedSeries) ts.Datapoints {
	datapoints := make(ts.Datapoints, len(r.Datapoints))
	for i, v := range r.Datapoints {
		datapoints[i] = ts.Datapoint{
			Timestamp: toTime(v.Timestamp),
			Value:     v.Value,
		}
	}

	return datapoints
}

// EncodeFetchRequest encodes fetch request into rpc FetchRequest
func EncodeFetchRequest(
	query *storage.FetchQuery,
) (*rpc.FetchRequest, error) {
	matchers, err := encodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	return &rpc.FetchRequest{
		Start: fromTime(query.Start),
		End:   fromTime(query.End),
		Matchers: &rpc.FetchRequest_TagMatchers{
			TagMatchers: matchers,
		},
	}, nil
}

func encodeTagMatchers(modelMatchers models.Matchers) (*rpc.TagMatchers, error) {
	matchers := make([]*rpc.TagMatcher, len(modelMatchers))
	for i, matcher := range modelMatchers {
		t, err := encodeMatcherTypeToProto(matcher.Type)
		if err != nil {
			return nil, err
		}

		matchers[i] = &rpc.TagMatcher{
			Name:  matcher.Name,
			Value: matcher.Value,
			Type:  t,
		}
	}

	return &rpc.TagMatchers{
		TagMatchers: matchers,
	}, nil
}

func encodeMatcherTypeToProto(t models.MatchType) (rpc.MatcherType, error) {
	switch t {
	case models.MatchEqual:
		return rpc.MatcherType_EQUAL, nil
	case models.MatchNotEqual:
		return rpc.MatcherType_NOTEQUAL, nil
	case models.MatchRegexp:
		return rpc.MatcherType_REGEXP, nil
	case models.MatchNotRegexp:
		return rpc.MatcherType_NOTREGEXP, nil
	default:
		return rpc.MatcherType_EQUAL, fmt.Errorf("Unknown matcher type for proto encoding")
	}
}

// EncodeMetadata creates a context that propagates request metadata as well as requestID
func EncodeMetadata(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		return ctx
	}

	headerValues := ctx.Value(handler.HeaderKey)
	headers, ok := headerValues.(http.Header)
	if !ok {
		return metadata.NewOutgoingContext(ctx, metadata.MD{reqIDKey: []string{requestID}})
	}

	return metadata.NewOutgoingContext(ctx, convertHeaderToMetaWithID(headers, requestID))
}

func convertHeaderToMetaWithID(headers http.Header, requestID string) metadata.MD {
	meta := make(metadata.MD, len(headers)+1)
	meta[reqIDKey] = []string{requestID}

	// Metadata keys must be in lowe case
	for k, v := range headers {
		meta[strings.ToLower(k)] = v
	}

	return meta
}

// RetrieveMetadata creates a context with propagated request metadata as well as requestID
func RetrieveMetadata(streamCtx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(streamCtx)
	id := "unknown"
	if ok {
		ids := md[reqIDKey]
		if len(ids) == 1 {
			id = ids[0]
		}
	}

	return logging.NewContextWithID(streamCtx, id)
}

// DecodeFetchRequest decodes rpc fetch request to read query and read options
func DecodeFetchRequest(
	req *rpc.FetchRequest,
) (*storage.FetchQuery, error) {
	tags, err := decodeTagMatchers(req.GetTagMatchers())
	if err != nil {
		return nil, err
	}

	return &storage.FetchQuery{
		TagMatchers: tags,
		Start:       toTime(req.Start),
		End:         toTime(req.End),
	}, nil
}

func decodeTagMatchers(rpcMatchers *rpc.TagMatchers) (models.Matchers, error) {
	tagMatchers := rpcMatchers.GetTagMatchers()
	matchers := make([]models.Matcher, len(tagMatchers))
	for i, matcher := range tagMatchers {
		matchType, name, value := models.MatchType(matcher.GetType()), matcher.GetName(), matcher.GetValue()
		mMatcher, err := models.NewMatcher(matchType, name, value)
		if err != nil {
			return matchers, err
		}

		matchers[i] = mMatcher
	}

	return models.Matchers(matchers), nil
}

// DecodeSearchRequest decodes rpc search request to read query and read options
func DecodeSearchRequest(
	req *rpc.SearchRequest,
) (*storage.FetchQuery, error) {
	return &storage.FetchQuery{}, nil
}
