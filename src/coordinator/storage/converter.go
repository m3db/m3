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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/util/execution"

	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/generated/proto/prompb"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

const (
	xTimeUnit = xtime.Millisecond
)

// PromWriteTSToM3 converts a prometheus write query to an M3 one
func PromWriteTSToM3(timeseries *prompb.TimeSeries) *WriteQuery {
	tags := PromLabelsToM3Tags(timeseries.Labels)
	datapoints := PromSamplesToM3Datapoints(timeseries.Samples)

	return &WriteQuery{
		Tags:       tags,
		Datapoints: datapoints,
		Unit:       xTimeUnit,
		Annotation: nil,
	}
}

// PromLabelsToM3Tags converts Prometheus labels to M3 tags
func PromLabelsToM3Tags(labels []*prompb.Label) models.Tags {
	tags := make(models.Tags, len(labels))
	for _, label := range labels {
		tags[label.Name] = label.Value
	}

	return tags
}

// PromSamplesToM3Datapoints converts Prometheus samples to M3 datapoints
func PromSamplesToM3Datapoints(samples []*prompb.Sample) ts.Datapoints {
	datapoints := make(ts.Datapoints, 0, len(samples))
	for _, sample := range samples {
		timestamp := TimestampToTime(sample.Timestamp)
		datapoints = append(datapoints, ts.Datapoint{Timestamp: timestamp, Value: sample.Value})
	}

	return datapoints
}

// PromReadQueryToM3 converts a prometheus read query to m3 ready query
func PromReadQueryToM3(query *prompb.Query) (*FetchQuery, error) {
	tagMatchers, err := PromMatchersToM3(query.Matchers)
	if err != nil {
		return nil, err
	}

	return &FetchQuery{
		TagMatchers: tagMatchers,
		Start:       TimestampToTime(query.StartTimestampMs),
		End:         TimestampToTime(query.EndTimestampMs),
	}, nil
}

// PromMatchersToM3 converts prometheus label matchers to m3 matchers
func PromMatchersToM3(matchers []*prompb.LabelMatcher) (models.Matchers, error) {
	tagMatchers := make(models.Matchers, len(matchers))
	var err error
	for idx, matcher := range matchers {
		tagMatchers[idx], err = PromMatcherToM3(matcher)
		if err != nil {
			return nil, err
		}
	}

	return tagMatchers, nil
}

// PromMatcherToM3 converts a prometheus label matcher to m3 matcher
func PromMatcherToM3(matcher *prompb.LabelMatcher) (*models.Matcher, error) {
	matchType, err := PromTypeToM3(matcher.Type)
	if err != nil {
		return nil, err
	}

	return models.NewMatcher(matchType, matcher.Name, matcher.Value)
}

// PromTypeToM3 converts a prometheus label type to m3 matcher type
func PromTypeToM3(labelType prompb.LabelMatcher_Type) (models.MatchType, error) {
	switch labelType {
	case prompb.LabelMatcher_EQ:
		return models.MatchEqual, nil
	case prompb.LabelMatcher_NEQ:
		return models.MatchNotEqual, nil
	case prompb.LabelMatcher_RE:
		return models.MatchRegexp, nil
	case prompb.LabelMatcher_NRE:
		return models.MatchNotRegexp, nil

	default:
		return 0, fmt.Errorf("unknown match type %v", labelType)
	}
}

// TimestampToTime converts a prometheus timestamp to time.Time
func TimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}

// TimeToTimestamp converts a time.Time to prometheus timestamp
func TimeToTimestamp(timestamp time.Time) int64 {
	return timestamp.UnixNano() / int64(time.Millisecond)
}

// FetchResultToPromResult converts fetch results from M3 to Prometheus result
func FetchResultToPromResult(result *FetchResult) *prompb.QueryResult {
	timeseries := make([]*prompb.TimeSeries, 0)

	for _, series := range result.SeriesList {
		promTs := SeriesToPromTS(series)
		timeseries = append(timeseries, promTs)
	}

	return &prompb.QueryResult{
		Timeseries: timeseries,
	}
}

// SeriesToPromTS converts a series to prometheus timeseries
func SeriesToPromTS(series *ts.Series) *prompb.TimeSeries {
	labels := TagsToPromLabels(series.Tags)
	samples := SeriesToPromSamples(series)
	return &prompb.TimeSeries{Labels: labels, Samples: samples}
}

// TagsToPromLabels converts tags to prometheus labels
func TagsToPromLabels(tags models.Tags) []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(tags))
	for k, v := range tags {
		labels = append(labels, &prompb.Label{Name: k, Value: v})
	}

	return labels
}

// SeriesToPromSamples series datapoints to prometheus samples
func SeriesToPromSamples(series *ts.Series) []*prompb.Sample {
	samples := make([]*prompb.Sample, series.Len())
	for i := 0; i < series.Len(); i++ {
		samples[i] = &prompb.Sample{
			Timestamp: series.Values().DatapointAt(i).Timestamp.UnixNano() / int64(time.Millisecond),
			Value:     series.Values().ValueAt(i),
		}
	}

	return samples
}

const (
	initRawFetchAllocSize = 32
	workerPoolSize        = 10
)

var (
	workerPool sync.WorkerPool
)

func init() {
	// Set up worker pool
	workerPool = sync.NewWorkerPool(workerPoolSize)
	workerPool.Init()
}

type decompressRequest struct {
	iter      encoding.SeriesIterator
	namespace ident.ID
	result    *ts.Series
}

func (w *decompressRequest) Process(ctx context.Context) error {
	iter := w.iter
	ns := w.namespace
	if ns == nil {
		ns = iter.Namespace()
	}

	metric, err := FromM3IdentToMetric(ns, iter.ID(), iter.Tags())
	if err != nil {
		return err
	}

	datapoints := make(ts.Datapoints, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		datapoints = append(datapoints, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
	}

	w.result = ts.NewSeries(metric.ID, datapoints, metric.Tags)
	return nil
}

// SeriesIteratorsToFetchResult converts SeriesIterators into a fetch result
func SeriesIteratorsToFetchResult(ctx context.Context, seriesIterators encoding.SeriesIterators, namespace ident.ID) (*FetchResult, error) {
	defer seriesIterators.Close()

	iters := seriesIterators.Iters()
	iterLength := seriesIterators.Len()

	seriesList := make([]*ts.Series, 0, iterLength)
	div, remainder := iterLength/workerPoolSize, iterLength%workerPoolSize
	var executionPools [][]execution.Request
	if remainder > 0 {
		executionPools = make([][]execution.Request, div+1)
		for i := 0; i < div; i++ {
			executionPools[i] = make([]execution.Request, workerPoolSize)
		}
		executionPools[div] = make([]execution.Request, remainder)
	} else {
		executionPools = make([][]execution.Request, div)
		for i := 0; i < div; i++ {
			executionPools[i] = make([]execution.Request, workerPoolSize)
		}
	}
	for idx, iter := range iters {
		executionPools[idx/workerPoolSize][idx%workerPoolSize] = &decompressRequest{iter: iter, namespace: namespace}
	}
	for _, executionPool := range executionPools {
		err := execution.ExecuteParallel(ctx, executionPool)
		if err != nil {
			return nil, err
		}
		for _, req := range executionPool {
			req, ok := req.(*decompressRequest)
			if !ok {
				return nil, errors.ErrBadRequestType
			}
			seriesList = append(seriesList, req.result)
		}
	}

	return &FetchResult{
		SeriesList: seriesList,
	}, nil
}
