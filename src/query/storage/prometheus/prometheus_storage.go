// Copyright (c) 2020 Uber Technologies, Inc.
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

package prometheus

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/instrument"
)

type prometheusQueryable struct {
	storage storage.Storage
	scope   tally.Scope
	logger  *zap.Logger
}

// PrometheusOptions are options to create a prometheus queryable backed by
// a m3 storage.
type PrometheusOptions struct {
	Storage           storage.Storage
	InstrumentOptions instrument.Options
}

// StorageErr wraps all errors returned by the storage layer.
// This allows the http handlers that call the Prometheus library directly to distinguish prometheus library errors
// and remote storage errors.
type StorageErr struct {
	inner error
}

// NewStorageErr wraps the provided error as a StorageErr.
func NewStorageErr(err error) *StorageErr {
	return &StorageErr{inner: err}
}

// Unwrap returns the underlying error.
func (e *StorageErr) Unwrap() error {
	return e.inner
}

// Error returns the error string for the underlying error.
func (e *StorageErr) Error() string {
	return e.inner.Error()
}

// NewPrometheusQueryable returns a new prometheus queryable backed by a m3
// storage.
func NewPrometheusQueryable(opts PrometheusOptions) promstorage.Queryable {
	scope := opts.InstrumentOptions.MetricsScope().Tagged(map[string]string{"storage": "prometheus_storage"})
	return &prometheusQueryable{
		storage: opts.Storage,
		scope:   scope,
		logger:  opts.InstrumentOptions.Logger(),
	}
}

// Querier returns a prometheus storage Querier.
func (q *prometheusQueryable) Querier(
	ctx context.Context, _, _ int64,
) (promstorage.Querier, error) {
	return newQuerier(ctx, q.storage, q.logger), nil
}

type querier struct {
	ctx     context.Context
	storage storage.Storage
	logger  *zap.Logger
}

func newQuerier(
	ctx context.Context,
	storage storage.Storage,
	logger *zap.Logger,
) promstorage.Querier {
	return &querier{
		ctx:     ctx,
		storage: storage,
		logger:  logger,
	}
}

func (q *querier) Select(
	sortSeries bool,
	hints *promstorage.SelectHints,
	labelMatchers ...*labels.Matcher,
) promstorage.SeriesSet {
	matchers, err := promql.LabelMatchersToModelMatcher(labelMatchers, models.NewTagOptions())
	if err != nil {
		return promstorage.ErrSeriesSet(err)
	}

	query := &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       time.Unix(0, hints.Start*int64(time.Millisecond)),
		End:         time.Unix(0, hints.End*int64(time.Millisecond)),
		Interval:    time.Duration(hints.Step) * time.Millisecond,
	}

	// NB (@shreyas): The fetch options builder sets it up from the request
	// which we do not have access to here.
	fetchOptions, err := fetchOptions(q.ctx)
	if err != nil {
		q.logger.Error("fetch options not provided in context", zap.Error(err))
		return promstorage.ErrSeriesSet(err)
	}

	result, err := q.storage.FetchProm(q.ctx, query, fetchOptions)
	if err != nil {
		return promstorage.ErrSeriesSet(NewStorageErr(err))
	}
	seriesSet := fromQueryResult(sortSeries, result.PromResult, result.Metadata)

	receiveResultMetadataFn, err := resultMetadataReceiveFn(q.ctx)
	if err != nil {
		q.logger.Error("result metadata not set in context", zap.Error(err))
		return promstorage.ErrSeriesSet(err)
	}
	if receiveResultMetadataFn == nil {
		err := errors.New("result metadata receive function nil for context")
		q.logger.Error(err.Error())
		return promstorage.ErrSeriesSet(err)
	}

	// Pass the result.Metadata back using the receive function.
	// This handles concurrent updates to a single result metadata.
	receiveResultMetadataFn(result.Metadata)

	return seriesSet
}

func (q *querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, promstorage.Warnings, error) {
	// TODO (@shreyas): Implement this.
	q.logger.Warn("calling unsupported LabelValues method")
	return nil, nil, errors.New("not implemented")
}

func (q *querier) LabelNames() ([]string, promstorage.Warnings, error) {
	// TODO (@shreyas): Implement this.
	q.logger.Warn("calling unsupported LabelNames method")
	return nil, nil, errors.New("not implemented")
}

func (q *querier) Close() error {
	return nil
}

func fromWarningStrings(warnings []string) []error {
	errs := make([]error, 0, len(warnings))
	for _, warning := range warnings {
		errs = append(errs, errors.New(warning))
	}
	return errs
}

// This is a copy of the prometheus remote.FromQueryResult method. Need to
// copy so that this can understand m3 prompb struct.
func fromQueryResult(sortSeries bool, res *prompb.QueryResult, metadata block.ResultMetadata) promstorage.SeriesSet {
	series := make([]promstorage.Series, 0, len(res.Timeseries))
	for _, ts := range res.Timeseries {
		labels := labelProtosToLabels(ts.Labels)
		if err := validateLabelsAndMetricName(labels); err != nil {
			return promstorage.ErrSeriesSet(err)
		}

		series = append(series, &concreteSeries{
			labels:  labels,
			samples: ts.Samples,
		})
	}

	if sortSeries {
		sort.Sort(byLabel(series))
	}

	warnings := fromWarningStrings(metadata.WarningStrings())

	return &concreteSeriesSet{
		series:   series,
		warnings: warnings,
	}
}

type byLabel []promstorage.Series

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }

func labelProtosToLabels(labelPairs []prompb.Label) labels.Labels {
	result := make(labels.Labels, 0, len(labelPairs))
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  string(l.Name),
			Value: string(l.Value),
		})
	}
	sort.Sort(result)
	return result
}

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() promstorage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur      int
	series   []promstorage.Series
	warnings promstorage.Warnings
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() promstorage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

func (c *concreteSeriesSet) Warnings() promstorage.Warnings {
	return c.warnings
}

// concreteSeries implements storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []prompb.Sample
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() chunkenc.Iterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) chunkenc.Iterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements storage.SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.Timestamp, s.Value
}

// Next implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read,
// also making sure that there are no labels with duplicate names
func validateLabelsAndMetricName(ls labels.Labels) error {
	for i, l := range ls {
		if l.Name == labels.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return errors.Errorf("invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return errors.Errorf("invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return errors.Errorf("invalid label value: %v", l.Value)
		}
		if i > 0 && l.Name == ls[i-1].Name {
			return errors.Errorf("duplicate label with name: %v", l.Name)
		}
	}
	return nil
}
