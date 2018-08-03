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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/query/generated/proto/prompb"
	"github.com/m3db/m3db/src/query/models"
	"github.com/m3db/m3db/src/query/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
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
	// TODO(arnikola) get from config
	initRawFetchAllocSize = 32
)

func iteratorToTsSeries(
	iter encoding.SeriesIterator,
	namespace ident.ID,
) (*ts.Series, error) {
	if namespace == nil {
		namespace = iter.Namespace()
	}
	metric, err := FromM3IdentToMetric(namespace, iter.ID(), iter.Tags())
	if err != nil {
		return nil, err
	}

	datapoints := make(ts.Datapoints, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		datapoints = append(datapoints, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
	}

	return ts.NewSeries(metric.ID, datapoints, metric.Tags), nil
}

// Fall back to sequential decompression if unable to decompress concurrently
func decompressSequentially(
	iterLength int,
	iters []encoding.SeriesIterator,
	namespace ident.ID,
) (*FetchResult, error) {
	seriesList := make([]*ts.Series, iterLength)
	for i, iter := range iters {
		series, err := iteratorToTsSeries(iter, namespace)
		if err != nil {
			return nil, err
		}
		seriesList[i] = series
	}

	return &FetchResult{
		SeriesList: seriesList,
	}, nil
}

func decompressConcurrently(
	iterLength int,
	iters []encoding.SeriesIterator,
	namespace ident.ID,
	pool xsync.WorkerPool,
) (*FetchResult, error) {
	seriesList := make([]*ts.Series, iterLength)
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)
	done := make(chan struct{})
	stopped := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	wg.Add(iterLength)

	for i, iter := range iters {
		i := i
		iter := iter
		pool.Go(func() {
			defer wg.Done()
			if stopped() {
				return
			}

			series, err := iteratorToTsSeries(iter, namespace)
			if err != nil {
				// Return the first error that is encountered.
				select {
				case errorCh <- err:
					close(done)
				default:
				}
				return
			}
			seriesList[i] = series
		})
	}
	wg.Wait()
	close(errorCh)
	if err := <-errorCh; err != nil {
		return nil, err
	}

	return &FetchResult{
		SeriesList: seriesList,
	}, nil
}

// SeriesIteratorsToFetchResult converts SeriesIterators into a fetch result
func SeriesIteratorsToFetchResult(
	seriesIterators encoding.SeriesIterators,
	namespace ident.ID,
	workerPools pool.ObjectPool,
) (*FetchResult, error) {
	defer seriesIterators.Close()

	iters := seriesIterators.Iters()
	iterLength := seriesIterators.Len()

	if workerPools == nil {
		return decompressSequentially(iterLength, iters, namespace)
	}

	pool, ok := workerPools.Get().(xsync.WorkerPool)
	if !ok {
		return decompressSequentially(iterLength, iters, namespace)
	}
	defer workerPools.Put(pool)

	return decompressConcurrently(iterLength, iters, namespace, pool)
}
