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
	"time"

	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3x/time"
)

func fromTime(t time.Time) int64 {
	return storage.TimeToTimestamp(t)
}

func toTime(t int64) time.Time {
	return storage.TimestampToTime(t)
}

func encodeTags(tagMap map[string]string) []*rpc.Tag {
	tags := make([]*rpc.Tag, 0, len(tagMap))
	for name, value := range tagMap {
		tags = append(tags, &rpc.Tag{
			Name:  []byte(name),
			Value: []byte(value),
		})
	}
	return tags
}

// EncodeFetchResult encodes fetch result to rpc result
func EncodeFetchResult(sResult *storage.FetchResult) *rpc.FetchResult {
	series := make([]*rpc.Series, len(sResult.SeriesList))
	for i, result := range sResult.SeriesList {
		vLen := result.Len()
		datapoints := make([]*rpc.Datapoint, vLen)
		_, fixedRes := result.Values().(ts.FixedResolutionMutableValues)
		for j := 0; j < vLen; j++ {
			dp := result.Values().DatapointAt(j)
			datapoints[j] = &rpc.Datapoint{
				Timestamp: fromTime(dp.Timestamp),
				Value:     dp.Value,
			}
		}

		series[i] = &rpc.Series{
			Id: []byte(result.Name()),
			Values: &rpc.Datapoints{
				Datapoints:      datapoints,
				FixedResolution: fixedRes,
			},
			Tags: encodeTags(result.Tags),
		}
	}
	return &rpc.FetchResult{Series: series}
}

// DecodeFetchResult decodes fetch results from a GRPC-compatible type.
func DecodeFetchResult(_ context.Context, rpcSeries []*rpc.Series) ([]*ts.Series, error) {
	tsSeries := make([]*ts.Series, len(rpcSeries))
	var err error
	for i, series := range rpcSeries {
		tsSeries[i], err = decodeTs(series)
		if err != nil {
			return nil, err
		}
	}
	return tsSeries, nil
}

func decodeTags(tags []*rpc.Tag) models.Tags {
	modelTags := make(models.Tags)
	for _, t := range tags {
		name, value := string(t.GetName()), string(t.GetValue())
		modelTags[name] = value
	}
	return modelTags
}

func decodeTs(r *rpc.Series) (*ts.Series, error) {
	fixedRes := r.Values.FixedResolution
	var (
		values ts.Values
		err    error
	)
	if fixedRes {
		values, err = decodeFixedResTs(r)
		if err != nil {
			return nil, err
		}

	} else {
		values = decodeRawTs(r)
	}

	tags := decodeTags(r.GetTags())
	series := ts.NewSeries(string(r.GetId()), values, tags)
	return series, nil
}

func decodeFixedResTs(r *rpc.Series) (ts.FixedResolutionMutableValues, error) {
	startTime := time.Time{}
	datapoints := r.Values.Datapoints
	if len(datapoints) > 0 {
		startTime = toTime(datapoints[0].Timestamp)
	}

	ms, ok := millisPerStep(r.Values)
	if !ok {
		return nil, fmt.Errorf("unable to find resolution")
	}
	values := ts.NewFixedStepValues(ms, len(datapoints), 0, startTime)
	for i, v := range datapoints {
		values.SetValueAt(i, v.Value)
	}
	return values, nil
}

func decodeRawTs(r *rpc.Series) ts.Datapoints {
	datapoints := make(ts.Datapoints, len(r.Values.Datapoints))
	for i, v := range r.Values.Datapoints {
		datapoints[i] = ts.Datapoint{
			Timestamp: toTime(v.Timestamp),
			Value:     v.Value,
		}
	}

	return datapoints
}

// EncodeFetchMessage encodes fetch query and fetch options into rpc WriteMessage
func EncodeFetchMessage(query *storage.FetchQuery, queryID string) *rpc.FetchMessage {
	return &rpc.FetchMessage{
		Query:   encodeFetchQuery(query),
		Options: encodeFetchOptions(queryID),
	}

}

func encodeFetchQuery(query *storage.FetchQuery) *rpc.FetchQuery {
	return &rpc.FetchQuery{
		Start:       fromTime(query.Start),
		End:         fromTime(query.End),
		TagMatchers: encodeTagMatchers(query.TagMatchers),
	}
}

func encodeTagMatchers(modelMatchers models.Matchers) []*rpc.Matcher {
	matchers := make([]*rpc.Matcher, len(modelMatchers))
	for i, matcher := range modelMatchers {
		matchers[i] = &rpc.Matcher{
			Name:  matcher.Name,
			Value: matcher.Value,
			Type:  int64(matcher.Type),
		}
	}

	return matchers
}

func encodeFetchOptions(queryID string) *rpc.FetchOptions {
	return &rpc.FetchOptions{
		Id: queryID,
	}
}

// DecodeFetchMessage decodes rpc fetch message to read query and read options
func DecodeFetchMessage(message *rpc.FetchMessage) (*storage.FetchQuery, string, error) {
	query, err := decodeFetchQuery(message.GetQuery())
	if err != nil {
		return nil, "", err
	}
	return query, message.GetOptions().GetId(), nil
}

func decodeFetchQuery(query *rpc.FetchQuery) (*storage.FetchQuery, error) {
	tags, err := decodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	return &storage.FetchQuery{
		TagMatchers: tags,
		Start:       toTime(query.Start),
		End:         toTime(query.End),
	}, nil
}

func decodeTagMatchers(rpcMatchers []*rpc.Matcher) (models.Matchers, error) {
	matchers := make([]*models.Matcher, len(rpcMatchers))
	for i, matcher := range rpcMatchers {
		matchType, name, value := models.MatchType(matcher.GetType()), matcher.GetName(), matcher.GetValue()
		mMatcher, err := models.NewMatcher(matchType, name, value)
		if err != nil {
			return matchers, err
		}
		matchers[i] = mMatcher
	}
	return models.Matchers(matchers), nil
}

// EncodeWriteMessage encodes write query and write options into rpc WriteMessage
func EncodeWriteMessage(query *storage.WriteQuery, queryID string) *rpc.WriteMessage {
	return &rpc.WriteMessage{
		Query:   encodeWriteQuery(query),
		Options: encodeWriteOptions(queryID),
	}
}

func encodeWriteQuery(query *storage.WriteQuery) *rpc.WriteQuery {
	return &rpc.WriteQuery{
		Unit:       int32(query.Unit),
		Annotation: query.Annotation,
		Datapoints: encodeDatapoints(query.Datapoints),
		Tags:       query.Tags,
	}
}

// DecodeWriteMessage decodes rpc write message to write query and write options
func DecodeWriteMessage(message *rpc.WriteMessage) (*storage.WriteQuery, string) {
	return decodeWriteQuery(message.GetQuery()), message.GetOptions().GetId()
}

func decodeWriteQuery(query *rpc.WriteQuery) *storage.WriteQuery {
	points := make([]ts.Datapoint, len(query.GetDatapoints()))
	for i, point := range query.GetDatapoints() {
		points[i] = ts.Datapoint{
			Timestamp: toTime(point.GetTimestamp()),
			Value:     point.GetValue(),
		}
	}
	return &storage.WriteQuery{
		Tags:       query.GetTags(),
		Datapoints: ts.Datapoints(points),
		Unit:       xtime.Unit(query.GetUnit()),
		Annotation: query.Annotation,
	}
}

func encodeDatapoints(tsPoints ts.Datapoints) []*rpc.Datapoint {
	datapoints := make([]*rpc.Datapoint, len(tsPoints))
	for i, point := range tsPoints {
		datapoints[i] = &rpc.Datapoint{
			Timestamp: fromTime(point.Timestamp),
			Value:     point.Value,
		}
	}
	return datapoints
}

func encodeWriteOptions(queryID string) *rpc.WriteOptions {
	return &rpc.WriteOptions{
		Id: queryID,
	}
}

func millisPerStep(dps *rpc.Datapoints) (time.Duration, bool) {
	if !dps.FixedResolution {
		return time.Duration(0), false
	}

	points := dps.Datapoints
	if len(points) <= 1 {
		return time.Duration(0), true
	}

	return time.Duration(points[1].Timestamp-points[0].Timestamp) * time.Millisecond, true
}
