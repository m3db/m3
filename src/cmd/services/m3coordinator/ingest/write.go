// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingest

import (
	"context"
	"sync"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	unaggregatedStoragePolicy   = policy.NewStoragePolicy(0, xtime.Unit(0), 0)
	unaggregatedStoragePolicies = []policy.StoragePolicy{
		unaggregatedStoragePolicy,
	}
)

// DownsampleAndWriteIter is an interface that can be implemented to use
// the WriteBatch method.
type DownsampleAndWriteIter interface {
	Next() bool
	Current() (models.Tags, ts.Datapoints, ts.SeriesAttributes, xtime.Unit, []byte)
	Reset() error
	Error() error
}

// DownsamplerAndWriter is the interface for the downsamplerAndWriter which
// writes metrics to the downsampler as well as to storage in unaggregated form.
type DownsamplerAndWriter interface {
	Write(
		ctx context.Context,
		tags models.Tags,
		datapoints ts.Datapoints,
		unit xtime.Unit,
		annotation []byte,
		overrides WriteOptions,
	) error

	WriteBatch(
		ctx context.Context,
		iter DownsampleAndWriteIter,
		overrides WriteOptions,
	) BatchError

	Storage() storage.Storage
}

// BatchError allows for access to individual errors.
type BatchError interface {
	error
	Errors() []error
	LastError() error
}

// WriteOptions contains overrides for the downsampling mapping
// rules and storage policies for a given write.
type WriteOptions struct {
	DownsampleMappingRules []downsample.AutoMappingRule
	WriteStoragePolicies   []policy.StoragePolicy

	DownsampleOverride bool
	WriteOverride      bool
}

// downsamplerAndWriter encapsulates the logic for writing data to the downsampler,
// as well as in unaggregated form to storage.
type downsamplerAndWriter struct {
	store       storage.Storage
	downsampler downsample.Downsampler
	workerPool  xsync.PooledWorkerPool
}

// NewDownsamplerAndWriter creates a new downsampler and writer.
func NewDownsamplerAndWriter(
	store storage.Storage,
	downsampler downsample.Downsampler,
	workerPool xsync.PooledWorkerPool,
) DownsamplerAndWriter {
	return &downsamplerAndWriter{
		store:       store,
		downsampler: downsampler,
		workerPool:  workerPool,
	}
}

func (d *downsamplerAndWriter) Write(
	ctx context.Context,
	tags models.Tags,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	annotation []byte,
	overrides WriteOptions,
) error {
	multiErr := xerrors.NewMultiError()
	if d.shouldDownsample(overrides) {
		err := d.writeToDownsampler(tags, datapoints, unit, overrides)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	if d.shouldWrite(overrides) {
		err := d.writeToStorage(ctx, tags, datapoints, unit, annotation, overrides)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (d *downsamplerAndWriter) shouldWrite(
	overrides WriteOptions,
) bool {
	var (
		// Ensure storage set.
		storageExists = d.store != nil
		// Ensure using default storage policies or some storage policies set.
		useDefaultStoragePolicies = !overrides.WriteOverride
		// If caller tried to override the storage policies, make sure there's
		// at least one.
		_, writeOverride = d.writeOverrideStoragePolicies(overrides)
	)
	// Only write directly to storage if the store exists, and caller wants to
	// use the default storage policies, or they're trying to override the
	// storage policies and they've provided at least one override to do so.
	return storageExists && (useDefaultStoragePolicies || writeOverride)
}

func (d *downsamplerAndWriter) writeOverrideStoragePolicies(
	overrides WriteOptions,
) ([]policy.StoragePolicy, bool) {
	writeOverride := overrides.WriteOverride && len(overrides.WriteStoragePolicies) > 0
	if !writeOverride {
		return nil, false
	}
	return overrides.WriteStoragePolicies, true
}

func (d *downsamplerAndWriter) shouldDownsample(
	overrides WriteOptions,
) bool {
	var (
		downsamplerExists = d.downsampler != nil
		// If they didn't request the mapping rules to be overridden, then assume they want the default
		// ones.
		useDefaultMappingRules = !overrides.DownsampleOverride
		// If they did try and override the mapping rules, make sure they've provided at least one.
		_, downsampleOverride = d.downsampleOverrideRules(overrides)
	)
	// Only downsample if the downsampler exists, and they either want to use the default mapping
	// rules, or they're trying to override the mapping rules and they've provided at least one
	// override to do so.
	return downsamplerExists && (useDefaultMappingRules || downsampleOverride)
}

func (d *downsamplerAndWriter) downsampleOverrideRules(
	overrides WriteOptions,
) ([]downsample.AutoMappingRule, bool) {
	downsampleOverride := overrides.DownsampleOverride && len(overrides.DownsampleMappingRules) > 0
	if !downsampleOverride {
		return nil, false
	}
	return overrides.DownsampleMappingRules, true
}

func (d *downsamplerAndWriter) writeToDownsampler(
	tags models.Tags,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	overrides WriteOptions,
) error {
	// TODO(rartoul): MetricsAppender has a Finalize() method, but it does not actually reuse many
	// resources. If we can pool this properly we can get a nice speedup.
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	defer appender.Finalize()

	for _, tag := range tags.Tags {
		appender.AddTag(tag.Name, tag.Value)
	}

	if tags.Opts.IDSchemeType() == models.TypeGraphite {
		// NB(r): This is gross, but if this is a graphite metric then
		// we are going to set a special tag that means the downsampler
		// will write a graphite ID. This should really be plumbed
		// through the downsampler in general, but right now the aggregator
		// does not allow context to be attached to a metric so when it calls
		// back the context is lost currently.
		// TODO_FIX_GRAPHITE_TAGGING: Using this string constant to track
		// all places worth fixing this hack. There is at least one
		// other path where flows back to the coordinator from the aggregator
		// and this tag is interpreted, eventually need to handle more cleanly.
		appender.AddTag(downsample.MetricsOptionIDSchemeTagName,
			downsample.GraphiteIDSchemeTagValue)
	}

	var appenderOpts downsample.SampleAppenderOptions
	if downsampleMappingRuleOverrides, ok := d.downsampleOverrideRules(overrides); ok {
		appenderOpts = downsample.SampleAppenderOptions{
			Override: true,
			OverrideRules: downsample.SamplesAppenderOverrideRules{
				MappingRules: downsampleMappingRuleOverrides,
			},
		}
	}

	samplesAppender, err := appender.SamplesAppender(appenderOpts)
	if err != nil {
		return err
	}

	for _, dp := range datapoints {
		err := samplesAppender.AppendGaugeTimedSample(dp.Timestamp, dp.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *downsamplerAndWriter) writeToStorage(
	ctx context.Context,
	tags models.Tags,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	annotation []byte,
	overrides WriteOptions,
) error {
	storagePolicies, ok := d.writeOverrideStoragePolicies(overrides)
	if !ok {
		return d.writeWithOptions(ctx, storage.WriteQueryOptions{
			Tags:       tags,
			Datapoints: datapoints,
			Unit:       unit,
			Annotation: annotation,
			Attributes: storageAttributesFromPolicy(unaggregatedStoragePolicy),
		})
	}

	var (
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
	)

	for _, p := range storagePolicies {
		p := p // Capture for goroutine.

		wg.Add(1)
		d.workerPool.Go(func() {
			err := d.writeWithOptions(ctx, storage.WriteQueryOptions{
				Tags:       tags,
				Datapoints: datapoints,
				Unit:       unit,
				Annotation: annotation,
				Attributes: storageAttributesFromPolicy(p),
			})
			if err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			wg.Done()
		})
	}

	wg.Wait()
	return multiErr.FinalError()
}

func (d *downsamplerAndWriter) writeWithOptions(
	ctx context.Context,
	opts storage.WriteQueryOptions,
) error {
	writeQuery, err := storage.NewWriteQuery(opts)
	if err != nil {
		return err
	}
	return d.store.Write(ctx, writeQuery)
}

func (d *downsamplerAndWriter) WriteBatch(
	ctx context.Context,
	iter DownsampleAndWriteIter,
	overrides WriteOptions,
) BatchError {
	var (
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
		addError = func(err error) {
			errLock.Lock()
			multiErr = multiErr.Add(err)
			errLock.Unlock()
		}
	)

	if d.shouldWrite(overrides) {
		// Write unaggregated. Spin up all the background goroutines that make
		// network requests before we do the synchronous work of writing to the
		// downsampler.
		storagePolicies, ok := d.writeOverrideStoragePolicies(overrides)
		if !ok {
			storagePolicies = unaggregatedStoragePolicies
		}

		for iter.Next() {
			tags, datapoints, _, unit, annotation := iter.Current()
			for _, p := range storagePolicies {
				p := p // Capture for lambda.
				wg.Add(1)
				d.workerPool.Go(func() {
					err := d.writeWithOptions(ctx, storage.WriteQueryOptions{
						Tags:       tags,
						Datapoints: datapoints,
						Unit:       unit,
						Annotation: annotation,
						Attributes: storageAttributesFromPolicy(p),
					})
					if err != nil {
						addError(err)
					}
					wg.Done()
				})
			}
		}
	}

	// Iter does not need to be synchronized because even though we use it to spawn
	// many goroutines above, the iteration is always synchronous.
	resetErr := iter.Reset()
	if resetErr != nil {
		addError(resetErr)
	}

	if d.shouldDownsample(overrides) && resetErr == nil {
		if err := d.writeAggregatedBatch(iter, overrides); err != nil {
			addError(err)
		}
	}

	wg.Wait()
	if multiErr.NumErrors() == 0 {
		return nil
	}

	return multiErr
}

func (d *downsamplerAndWriter) writeAggregatedBatch(
	iter DownsampleAndWriteIter,
	overrides WriteOptions,
) error {
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	defer appender.Finalize()

	for iter.Next() {
		appender.Reset()
		tags, datapoints, info, _, _ := iter.Current()
		for _, tag := range tags.Tags {
			appender.AddTag(tag.Name, tag.Value)
		}

		var opts downsample.SampleAppenderOptions
		if downsampleMappingRuleOverrides, ok := d.downsampleOverrideRules(overrides); ok {
			opts = downsample.SampleAppenderOptions{
				Override: true,
				OverrideRules: downsample.SamplesAppenderOverrideRules{
					MappingRules: downsampleMappingRuleOverrides,
				},
			}
		}

		samplesAppender, err := appender.SamplesAppender(opts)
		if err != nil {
			return err
		}

		for _, dp := range datapoints {
			switch info.Type {
			case ts.MetricTypeGauge:
				err = samplesAppender.AppendGaugeTimedSample(dp.Timestamp, dp.Value)
			case ts.MetricTypeCounter:
				err = samplesAppender.AppendCounterTimedSample(dp.Timestamp, int64(dp.Value))
			case ts.MetricTypeTimer:
				err = samplesAppender.AppendTimerTimedSample(dp.Timestamp, dp.Value)
			}
			if err != nil {
				return err
			}
		}
	}

	return iter.Error()
}

func (d *downsamplerAndWriter) Storage() storage.Storage {
	return d.store
}

func storageAttributesFromPolicy(
	p policy.StoragePolicy,
) consolidators.Attributes {
	attributes := consolidators.Attributes{
		MetricsType: storage.UnaggregatedMetricsType,
	}
	if p != unaggregatedStoragePolicy {
		attributes = consolidators.Attributes{
			// Assume all overridden storage policies are for aggregated namespaces.
			MetricsType: consolidators.AggregatedMetricsType,
			Resolution:  p.Resolution().Window,
			Retention:   p.Retention().Duration(),
		}
	}
	return attributes
}
