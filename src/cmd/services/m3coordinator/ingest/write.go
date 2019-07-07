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
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
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
	TagOptions() models.TagOptions

	Next() bool

	Current() (ident.TagIterator, ts.Datapoints, xtime.Unit)

	Err() error

	DatapointResult(datapointIdx int) storage.WriteQueryResult
	DatapointState(datapointIdx int) interface{}

	SetDatapointResult(datapointIdx int, result storage.WriteQueryResult)
	SetDatapointState(datapointIdx int, state interface{})

	Restart()
}

// DownsamplerAndWriter is the interface for the downsamplerAndWriter which
// writes metrics to the downsampler as well as to storage in unaggregated form.
type DownsamplerAndWriter interface {
	Write(
		ctx context.Context,
		tags ident.TagIterator,
		tagOptions models.TagOptions,
		datapoints ts.Datapoints,
		unit xtime.Unit,
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
	DownsampleMappingRules []downsample.MappingRule
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
	tags ident.TagIterator,
	tagOptions models.TagOptions,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	overrides WriteOptions,
) error {
	multiErr := xerrors.NewMultiError()
	if d.shouldDownsample(overrides) {
		err := d.writeToDownsampler(tags, tagOptions, datapoints, unit, overrides)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	if d.shouldWrite(overrides) {
		err := d.writeToStorage(ctx, tags, tagOptions, datapoints, unit, overrides)
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
) ([]downsample.MappingRule, bool) {
	downsampleOverride := overrides.DownsampleOverride && len(overrides.DownsampleMappingRules) > 0
	if !downsampleOverride {
		return nil, false
	}
	return overrides.DownsampleMappingRules, true
}

func (d *downsamplerAndWriter) writeToDownsampler(
	tags ident.TagIterator,
	tagOptions models.TagOptions,
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

	if tagOptions.IDSchemeType() == models.TypeGraphite {
		// NB(r): This is gross, but if this is a graphite metric then
		// we are going to set a special tag that means the downsampler
		// will write a graphite ID. This should really be plumbed
		// through the downsampler in general, but right now the aggregator
		// does not allow context to be attached to a metric so when it calls
		// back the context is lost currently.
		appender.AddTag(downsample.MetricsOptionIDSchemeTagName,
			downsample.GraphiteIDSchemeTagValue)
	}

	// Duplicate so other iterators can cleanly iterate.
	duplicate := tags.Duplicate()
	for duplicate.Next() {
		tag := duplicate.Current()
		appender.AddTag(tag.Name.Bytes(), tag.Value.Bytes())
	}
	err = duplicate.Err()
	duplicate.Close()
	if err != nil {
		return err
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
	tags ident.TagIterator,
	tagOptions models.TagOptions,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	overrides WriteOptions,
) error {
	storagePolicies, ok := d.writeOverrideStoragePolicies(overrides)
	if !ok {
		// Duplicate so others can iterate in order.
		duplicate := tags.Duplicate()

		write := storage.NewWriteQuery(storage.WriteQueryOptions{
			Tags:       duplicate,
			TagOptions: tagOptions,
			Unit:       unit,
			Attributes: storageAttributesFromPolicy(unaggregatedStoragePolicy),
		})
		write.AppendDatapoints(datapoints)
		err := d.store.Write(ctx, write)
		duplicate.Close()
		return err
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
			// Duplicate so others can iterate in order.
			duplicate := tags.Duplicate()

			write := storage.NewWriteQuery(storage.WriteQueryOptions{
				Tags:       duplicate,
				TagOptions: tagOptions,
				Unit:       unit,
				Attributes: storageAttributesFromPolicy(p),
			})
			write.AppendDatapoints(datapoints)
			if err := d.store.Write(ctx, write); err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			duplicate.Close()
			wg.Done()
		})
	}

	wg.Wait()
	return multiErr.FinalError()
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
		// Write to underlying storage for each policy.
		// TODO(r): Parallelize writing each policy at once, this is difficult
		// though as right now the iterator expect sequential access.
		// In reality the unaggregated storage policy or a single storage
		// policy is selected so this isn't a huge deal.
		storagePolicies, ok := d.writeOverrideStoragePolicies(overrides)
		if !ok {
			storagePolicies = unaggregatedStoragePolicies
		}

		writeIter := NewWriteQueryIter(iter)
		for _, p := range storagePolicies {
			writeIter.Reset(storageAttributesFromPolicy(p))
			if err := d.store.WriteBatch(ctx, writeIter); err != nil {
				addError(err)
			}
		}
	}

	if d.shouldDownsample(overrides) {
		iter.Restart()
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
		tags, datapoints, _ := iter.Current()

		// Duplicate so other iterators can cleanly iterate.
		tags.Restart()
		for tags.Next() {
			tag := tags.Current()
			appender.AddTag(tag.Name.Bytes(), tag.Value.Bytes())
		}
		if err := tags.Err(); err != nil {
			return err
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
			err := samplesAppender.AppendGaugeTimedSample(dp.Timestamp, dp.Value)
			if err != nil {
				return err
			}
		}
	}

	return iter.Err()
}

func (d *downsamplerAndWriter) Storage() storage.Storage {
	return d.store
}

func storageAttributesFromPolicy(
	p policy.StoragePolicy,
) storage.Attributes {
	attributes := storage.Attributes{
		MetricsType: storage.UnaggregatedMetricsType,
	}
	if p != unaggregatedStoragePolicy {
		attributes = storage.Attributes{
			// Assume all overridden storage policies are for aggregated namespaces.
			MetricsType: storage.AggregatedMetricsType,
			Resolution:  p.Resolution().Window,
			Retention:   p.Retention().Duration(),
		}
	}
	return attributes
}

var _ WriteQueryIter = &writeQueryIter{}

// WriteQueryIter is a storage write query iterator with
// reset methods.
type WriteQueryIter interface {
	storage.WriteQueryIter
	Reset(attr storage.Attributes)
}

type writeQueryIter struct {
	iter  DownsampleAndWriteIter
	attr  storage.Attributes
	attrs [1]storage.Attributes
	slice []storage.Attributes
}

// NewWriteQueryIter is used to create a storage write query iterator
// from a downsample and write iterator.
func NewWriteQueryIter(
	iter DownsampleAndWriteIter,
) WriteQueryIter {
	return &writeQueryIter{
		iter: iter,
	}
}

func (i *writeQueryIter) Reset(attr storage.Attributes) {
	i.attr = attr
	i.attrs[0] = attr
	i.slice = i.attrs[:]
}

func (i *writeQueryIter) UniqueAttributes() []storage.Attributes {
	return i.slice
}

func (i *writeQueryIter) Next() bool {
	return i.iter.Next()
}

func (i *writeQueryIter) Current() storage.WriteQuery {
	tags, datapoints, unit := i.iter.Current()

	// Reset tags for reuse.
	tags.Restart()
	write := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags:       tags,
		TagOptions: i.iter.TagOptions(),
		Unit:       unit,
		Attributes: i.attr,
	})
	write.AppendDatapoints(datapoints)
	return write
}

func (i *writeQueryIter) CurrentAttributes() storage.Attributes {
	return i.attr
}

func (i *writeQueryIter) Err() error {
	return i.iter.Err()
}

func (i *writeQueryIter) DatapointResult(datapointIdx int) storage.WriteQueryResult {
	return i.iter.DatapointResult(datapointIdx)
}

func (i *writeQueryIter) DatapointState(datapointIdx int) interface{} {
	return i.iter.DatapointState(datapointIdx)
}

func (i *writeQueryIter) SetDatapointResult(datapointIdx int, result storage.WriteQueryResult) {
	i.iter.SetDatapointResult(datapointIdx, result)
}

func (i *writeQueryIter) SetDatapointState(datapointIdx int, state interface{}) {
	i.iter.SetDatapointState(datapointIdx, state)
}

func (i *writeQueryIter) Restart() {
	i.iter.Restart()
}

func (i *writeQueryIter) Close() {
	// Noop.
}
