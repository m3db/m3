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
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/proto/annotation"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"

	"github.com/prometheus/common/model"
)

var (
	// The default name for the name and bucket tags in Prometheus metrics.
	// This can be overwritten by setting tagOptions in the config.
	promDefaultName       = []byte(model.MetricNameLabel) // __name__
	promDefaultBucketName = []byte(model.BucketLabel)     // le

	// The suffix of count metric name in Prometheus histogram/summary metric families.
	promDefaultCountSuffix = []byte("_count")
)

// PromLabelsToM3Tags converts Prometheus labels to M3 tags
func PromLabelsToM3Tags(
	labels []prompb.Label,
	tagOptions models.TagOptions,
) models.Tags {
	tags := models.NewTags(len(labels), tagOptions)
	tagList := make([]models.Tag, 0, len(labels))
	for _, label := range labels {
		name := label.Name
		// If this label corresponds to the Prometheus name or bucket name,
		// instead set it as the given name tag from the config file.
		if bytes.Equal(promDefaultName, name) {
			tags = tags.SetName(label.Value)
		} else if bytes.Equal(promDefaultBucketName, name) {
			tags = tags.SetBucket(label.Value)
		} else {
			tagList = append(tagList, models.Tag{
				Name:  name,
				Value: label.Value,
			})
		}
	}

	return tags.AddTags(tagList)
}

// PromTimeSeriesToSeriesAttributes extracts the series info from a prometheus
// timeseries.
func PromTimeSeriesToSeriesAttributes(series prompb.TimeSeries) (ts.SeriesAttributes, error) {
	var (
		sourceType        ts.SourceType
		m3MetricType      ts.M3MetricType
		promMetricType    ts.PromMetricType
		handleValueResets bool
	)

	switch series.Source {
	case prompb.Source_PROMETHEUS:
		sourceType = ts.SourceTypePrometheus
	case prompb.Source_GRAPHITE:
		sourceType = ts.SourceTypeGraphite
	default:
		return ts.SeriesAttributes{}, fmt.Errorf("invalid source type %v", series.Source)
	}

	switch series.Type {

	case prompb.MetricType_UNKNOWN:
		promMetricType = ts.PromMetricTypeUnknown

	case prompb.MetricType_COUNTER:
		promMetricType = ts.PromMetricTypeCounter
		handleValueResets = true

	case prompb.MetricType_GAUGE:
		promMetricType = ts.PromMetricTypeGauge

	case prompb.MetricType_HISTOGRAM:
		promMetricType = ts.PromMetricTypeHistogram
		handleValueResets = true

	case prompb.MetricType_GAUGE_HISTOGRAM:
		promMetricType = ts.PromMetricTypeGaugeHistogram
		name := metricNameFromLabels(series.Labels)
		handleValueResets = bytes.HasSuffix(name, promDefaultCountSuffix)

	case prompb.MetricType_SUMMARY:
		promMetricType = ts.PromMetricTypeSummary
		handleValueResets = true

	case prompb.MetricType_INFO:
		promMetricType = ts.PromMetricTypeInfo

	case prompb.MetricType_STATESET:
		promMetricType = ts.PromMetricTypeStateSet

	default:
		return ts.SeriesAttributes{}, fmt.Errorf("invalid Prometheus metric type %v", series.Type)
	}

	switch series.M3Type {
	case prompb.M3Type_M3_COUNTER:
		m3MetricType = ts.M3MetricTypeCounter
		if promMetricType == ts.PromMetricTypeUnknown && series.Source == prompb.Source_GRAPHITE {
			promMetricType = ts.PromMetricTypeCounter
		}
	case prompb.M3Type_M3_GAUGE:
		m3MetricType = ts.M3MetricTypeGauge
		if promMetricType == ts.PromMetricTypeUnknown && series.Source == prompb.Source_GRAPHITE {
			promMetricType = ts.PromMetricTypeGauge
		}
	case prompb.M3Type_M3_TIMER:
		m3MetricType = ts.M3MetricTypeTimer
	default:
		return ts.SeriesAttributes{}, fmt.Errorf("invalid M3 metric type %v", series.M3Type)
	}

	return ts.SeriesAttributes{
		M3Type:            m3MetricType,
		PromType:          promMetricType,
		Source:            sourceType,
		HandleValueResets: handleValueResets,
	}, nil
}

// SeriesAttributesToAnnotationPayload converts passed arguments into an annotation.Payload.
func SeriesAttributesToAnnotationPayload(
	promType ts.PromMetricType,
	handleValueResets bool) (annotation.Payload, error) {
	var metricType annotation.MetricType
	switch promType {
	case ts.PromMetricTypeUnknown:
		metricType = annotation.MetricType_UNKNOWN

	case ts.PromMetricTypeCounter:
		metricType = annotation.MetricType_COUNTER

	case ts.PromMetricTypeGauge:
		metricType = annotation.MetricType_GAUGE

	case ts.PromMetricTypeHistogram:
		metricType = annotation.MetricType_HISTOGRAM

	case ts.PromMetricTypeGaugeHistogram:
		metricType = annotation.MetricType_GAUGE_HISTOGRAM

	case ts.PromMetricTypeSummary:
		metricType = annotation.MetricType_SUMMARY

	case ts.PromMetricTypeInfo:
		metricType = annotation.MetricType_INFO

	case ts.PromMetricTypeStateSet:
		metricType = annotation.MetricType_STATESET

	default:
		return annotation.Payload{}, fmt.Errorf("invalid Prometheus metric type %v", promType)
	}

	return annotation.Payload{
		MetricType:        metricType,
		HandleValueResets: handleValueResets,
	}, nil
}

// PromSamplesToM3Datapoints converts Prometheus samples to M3 datapoints
func PromSamplesToM3Datapoints(samples []prompb.Sample) ts.Datapoints {
	datapoints := make(ts.Datapoints, 0, len(samples))
	for _, sample := range samples {
		timestamp := PromTimestampToTime(sample.Timestamp)
		datapoints = append(datapoints, ts.Datapoint{Timestamp: timestamp, Value: sample.Value})
	}

	return datapoints
}

// PromReadQueryToM3 converts a prometheus read query to m3 read query
func PromReadQueryToM3(query *prompb.Query) (*FetchQuery, error) {
	tagMatchers, err := PromMatchersToM3(query.Matchers)
	if err != nil {
		return nil, err
	}

	start := PromTimestampToTime(query.StartTimestampMs)
	end := PromTimestampToTime(query.EndTimestampMs)
	if start.After(end) {
		start = time.Time{}
		end = time.Now()
	}

	return &FetchQuery{
		TagMatchers: tagMatchers,
		Start:       start,
		End:         end,
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
func PromMatcherToM3(matcher *prompb.LabelMatcher) (models.Matcher, error) {
	matchType, err := PromTypeToM3(matcher.Type)
	if err != nil {
		return models.Matcher{}, err
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
		return 0, fmt.Errorf("unknown match type: %v", labelType)
	}
}

// PromTimestampToTime converts a prometheus timestamp to time.Time.
func PromTimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}

// TimeToPromTimestamp converts a time.Time to prometheus timestamp.
func TimeToPromTimestamp(timestamp time.Time) int64 {
	// Significantly faster than time.Truncate()
	return timestamp.UnixNano() / int64(time.Millisecond)
}

// FetchResultToPromResult converts fetch results from M3 to Prometheus result.
func FetchResultToPromResult(
	result *FetchResult,
	keepEmpty bool,
) *prompb.QueryResult {
	// Perform bulk allocation upfront then convert to pointers afterwards
	// to reduce total number of allocations. See BenchmarkFetchResultToPromResult
	// if modifying.
	timeseries := make([]prompb.TimeSeries, 0, len(result.SeriesList))
	for _, series := range result.SeriesList {
		if !keepEmpty && series.Len() == 0 {
			continue
		}

		promTs := SeriesToPromTS(series)
		timeseries = append(timeseries, promTs)
	}

	timeSeriesPointers := make([]*prompb.TimeSeries, 0, len(result.SeriesList))
	for i := range timeseries {
		timeSeriesPointers = append(timeSeriesPointers, &timeseries[i])
	}

	return &prompb.QueryResult{
		Timeseries: timeSeriesPointers,
	}
}

// SeriesToPromTS converts a series to prometheus timeseries.
func SeriesToPromTS(series *ts.Series) prompb.TimeSeries {
	labels := TagsToPromLabels(series.Tags)
	samples := SeriesToPromSamples(series)
	return prompb.TimeSeries{Labels: labels, Samples: samples}
}

type sortableLabels []prompb.Label

func (t sortableLabels) Len() int      { return len(t) }
func (t sortableLabels) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t sortableLabels) Less(i, j int) bool {
	return bytes.Compare(t[i].Name, t[j].Name) == -1
}

// TagsToPromLabels converts tags to prometheus labels.
func TagsToPromLabels(tags models.Tags) []prompb.Label {
	l := tags.Len()
	labels := make([]prompb.Label, 0, l)

	metricName := tags.Opts.MetricName()
	bucketName := tags.Opts.BucketName()
	for _, t := range tags.Tags {
		if bytes.Equal(t.Name, metricName) {
			labels = append(labels,
				prompb.Label{Name: promDefaultName, Value: t.Value})
		} else if bytes.Equal(t.Name, bucketName) {
			labels = append(labels,
				prompb.Label{Name: promDefaultBucketName, Value: t.Value})
		} else {
			labels = append(labels, prompb.Label{Name: t.Name, Value: t.Value})
		}
	}

	// Sort here since name and label may be added in a different order in tags
	// if default metric name or bucket names are overridden.
	sort.Sort(sortableLabels(labels))

	return labels
}

// SeriesToPromSamples series datapoints to prometheus samples.SeriesToPromSamples.
func SeriesToPromSamples(series *ts.Series) []prompb.Sample {
	var (
		seriesLen  = series.Len()
		values     = series.Values()
		datapoints = values.Datapoints()
		samples    = make([]prompb.Sample, 0, seriesLen)
	)
	for _, dp := range datapoints {
		samples = append(samples, prompb.Sample{
			Timestamp: TimeToPromTimestamp(dp.Timestamp),
			Value:     dp.Value,
		})
	}

	return samples
}

func metricNameFromLabels(labels []prompb.Label) []byte {
	for _, label := range labels {
		if bytes.Equal(promDefaultName, label.GetName()) {
			return label.GetValue()
		}
	}
	return nil
}
