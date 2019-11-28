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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/rpcpb"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"google.golang.org/grpc/metadata"
)

const reqIDKey = "reqid"

func fromTime(t time.Time) int64 {
	return storage.TimeToPromTimestamp(t)
}

func toTime(t int64) time.Time {
	return storage.PromTimestampToTime(t)
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

// encodeFetchResult  encodes fetch result to rpc response
func encodeFetchResult(results *storage.FetchResult) *rpc.FetchResponse {
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
				Id: result.Name(),
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
		Meta:   encodeResultMetadata(results.Metadata),
	}
}

// encodeFetchRequest encodes fetch request into rpc FetchRequest
func encodeFetchRequest(
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*rpc.FetchRequest, error) {
	matchers, err := encodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	opts, err := encodeFetchOptions(options)
	if err != nil {
		return nil, err
	}

	return &rpc.FetchRequest{
		Start: fromTime(query.Start),
		End:   fromTime(query.End),
		Matchers: &rpc.FetchRequest_TagMatchers{
			TagMatchers: matchers,
		},
		Options: opts,
	}, nil
}

func encodeTagMatchers(modelMatchers models.Matchers) (*rpc.TagMatchers, error) {
	matchers := make([]*rpc.TagMatcher, len(modelMatchers))
	for i, matcher := range modelMatchers {
		fmt.Println("matcher", matcher)
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

func encodeFanoutOption(opt storage.FanoutOption) (rpc.FanoutOption, error) {
	switch opt {
	case storage.FanoutDefault:
		return rpc.FanoutOption_DEFAULT_OPTION, nil
	case storage.FanoutForceDisable:
		return rpc.FanoutOption_FORCE_DISABLED, nil
	case storage.FanoutForceEnable:
		return rpc.FanoutOption_FORCE_ENABLED, nil
	}

	return 0, fmt.Errorf("unknown fanout option for proto encoding: %v", opt)
}

func encodeFetchOptions(options *storage.FetchOptions) (*rpc.FetchOptions, error) {
	if options == nil {
		return nil, nil
	}

	fanoutOpts := options.FanoutOptions
	result := &rpc.FetchOptions{
		Limit:             int64(options.Limit),
		IncludeResolution: options.IncludeResolution,
	}

	unagg, err := encodeFanoutOption(fanoutOpts.FanoutUnaggregated)
	if err != nil {
		return nil, err
	}

	result.Unaggregated = unagg
	agg, err := encodeFanoutOption(fanoutOpts.FanoutAggregated)
	if err != nil {
		return nil, err
	}

	result.Aggregated = agg
	aggOpt, err := encodeFanoutOption(fanoutOpts.FanoutAggregatedOptimized)
	if err != nil {
		return nil, err
	}

	result.AggregatedOptimized = aggOpt
	if v := options.RestrictFetchOptions; v != nil {
		restrict, err := encodeRestrictFetchOptions(v)
		if err != nil {
			return nil, err
		}

		result.Restrict = restrict
	}

	if v := options.LookbackDuration; v != nil {
		result.LookbackDuration = int64(*v)
	}

	return result, nil
}

func encodeRestrictFetchOptions(
	o *storage.RestrictFetchOptions,
) (*rpcpb.RestrictFetchOptions, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}

	result := &rpcpb.RestrictFetchOptions{}

	switch o.MetricsType {
	case storage.UnaggregatedMetricsType:
		result.MetricsType = rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE
	case storage.AggregatedMetricsType:
		result.MetricsType = rpcpb.MetricsType_AGGREGATED_METRICS_TYPE

		storagePolicyProto, err := o.StoragePolicy.Proto()
		if err != nil {
			return nil, err
		}

		result.MetricsStoragePolicy = storagePolicyProto
	}

	if len(o.MustApplyMatchers) > 0 {
		fmt.Println("Applhying matchers", o.MustApplyMatchers)
		matchers, err := encodeTagMatchers(o.MustApplyMatchers)
		if err != nil {
			return nil, err
		}
		fmt.Println(" matchers", matchers)

		result.MustApplyMatchers = matchers
	}

	return result, nil
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
	case models.MatchField:
		return rpc.MatcherType_EXISTS, nil
	case models.MatchNotField:
		return rpc.MatcherType_NOTEXISTS, nil
	case models.MatchAll:
		return rpc.MatcherType_ALL, nil
	default:
		return 0, fmt.Errorf("unknown matcher type for proto encoding")
	}
}

// encodeMetadata creates a context that propagates request metadata as well as requestID
func encodeMetadata(ctx context.Context, requestID string) context.Context {
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

// creates a context with propagated request metadata as well as requestID
func retrieveMetadata(
	streamCtx context.Context,
	instrumentOpts instrument.Options,
) context.Context {
	md, ok := metadata.FromIncomingContext(streamCtx)
	id := "unknown"
	if ok {
		ids := md[reqIDKey]
		if len(ids) == 1 {
			id = ids[0]
		}
	}

	return logging.NewContextWithID(streamCtx, id, instrumentOpts)
}

func decodeFetchRequest(
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

func decodeFanoutOption(opt rpc.FanoutOption) (storage.FanoutOption, error) {
	switch opt {
	case rpc.FanoutOption_DEFAULT_OPTION:
		return storage.FanoutDefault, nil
	case rpc.FanoutOption_FORCE_DISABLED:
		return storage.FanoutForceDisable, nil
	case rpc.FanoutOption_FORCE_ENABLED:
		return storage.FanoutForceEnable, nil
	}

	return 0, fmt.Errorf("unknown fanout option for proto encoding: %v", opt)
}

func decodeRestrictFetchOptions(
	p *rpc.RestrictFetchOptions,
) (storage.RestrictFetchOptions, error) {
	var result storage.RestrictFetchOptions

	if p == nil {
		return result, errors.New("no restrict fetch options proto message")
	}

	switch p.GetMetricsType() {
	case rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE:
		result.MetricsType = storage.UnaggregatedMetricsType
	case rpcpb.MetricsType_AGGREGATED_METRICS_TYPE:
		result.MetricsType = storage.AggregatedMetricsType
	}

	if p.GetMetricsStoragePolicy() != nil {
		storagePolicy, err := policy.NewStoragePolicyFromProto(
			p.MetricsStoragePolicy)
		if err != nil {
			return result, err
		}

		result.StoragePolicy = storagePolicy
	}

	matchers := p.GetMustApplyMatchers()
	if len(matchers.GetTagMatchers()) > 0 {
		decodedMatchers, err := decodeTagMatchers(matchers)
		if err != nil {
			return result, err
		}

		result.MustApplyMatchers = decodedMatchers
	}

	// Validate the resulting options.
	if err := result.Validate(); err != nil {
		return result, err
	}

	return result, nil
}

func decodeFetchOptions(rpcFetchOptions *rpc.FetchOptions) (*storage.FetchOptions, error) {
	result := storage.NewFetchOptions()
	result.Remote = true
	if rpcFetchOptions == nil {
		return result, nil
	}

	result.Limit = int(rpcFetchOptions.Limit)
	result.IncludeResolution = rpcFetchOptions.GetIncludeResolution()
	unagg, err := decodeFanoutOption(rpcFetchOptions.GetUnaggregated())
	if err != nil {
		return nil, err
	}

	agg, err := decodeFanoutOption(rpcFetchOptions.GetAggregated())
	if err != nil {
		return nil, err
	}

	aggOpt, err := decodeFanoutOption(rpcFetchOptions.GetAggregatedOptimized())
	if err != nil {
		return nil, err
	}

	result.FanoutOptions = &storage.FanoutOptions{
		FanoutUnaggregated:        unagg,
		FanoutAggregated:          agg,
		FanoutAggregatedOptimized: aggOpt,
	}

	if v := rpcFetchOptions.Restrict; v != nil {
		restrict, err := decodeRestrictFetchOptions(v)
		if err != nil {
			return nil, err
		}

		result.RestrictFetchOptions = &restrict
	}

	if v := rpcFetchOptions.LookbackDuration; v > 0 {
		duration := time.Duration(v)
		result.LookbackDuration = &duration
	}

	return result, nil
}

func encodeResultMetadata(meta block.ResultMetadata) *rpc.ResultMetadata {
	warnings := make([]*rpc.Warning, 0, len(meta.Warnings))
	for _, warn := range meta.Warnings {
		warnings = append(warnings, &rpc.Warning{
			Name:    []byte(warn.Name),
			Message: []byte(warn.Message),
		})
	}

	return &rpc.ResultMetadata{
		Exhaustive:  meta.Exhaustive,
		Warnings:    warnings,
		Resolutions: meta.Resolutions,
	}
}

func decodeResultMetadata(meta *rpc.ResultMetadata) block.ResultMetadata {
	rpcWarnings := meta.GetWarnings()
	warnings := make([]block.Warning, 0, len(rpcWarnings))
	for _, warn := range rpcWarnings {
		warnings = append(warnings, block.Warning{
			Name:    string(warn.Name),
			Message: string(warn.Message),
		})
	}

	return block.ResultMetadata{
		Exhaustive:  meta.Exhaustive,
		Warnings:    warnings,
		Resolutions: meta.GetResolutions(),
	}
}
