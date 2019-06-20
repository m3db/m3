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

// DownsampleAndWriteIter is an interface that can be implemented to use
// the WriteBatch method.
type DownsampleAndWriteIter interface {
	TagOptions() models.TagOptions
	Next() bool
	Current() (ident.TagIterator, ts.Datapoints, xtime.Unit)
	Reset() error
	Error() error
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

	// TODO(rartoul): Batch interface should also support downsampling rules.
	WriteBatch(
		ctx context.Context,
		iter DownsampleAndWriteIter,
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
	err := d.maybeWriteDownsampler(tags, tagOptions, datapoints, unit, overrides)
	if err != nil {
		return err
	}

	return d.maybeWriteStorage(ctx, tags, tagOptions, datapoints, unit, overrides)
}

func (d *downsamplerAndWriter) maybeWriteDownsampler(
	tags ident.TagIterator,
	tagOptions models.TagOptions,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	overrides WriteOptions,
) error {
	var (
		downsamplerExists = d.downsampler != nil
		// If they didn't request the mapping rules to be overridden, then assume they want the default
		// ones.
		useDefaultMappingRules = !overrides.DownsampleOverride
		// If they did try and override the mapping rules, make sure they've provided at least one.
		downsampleOverride = overrides.DownsampleOverride && len(overrides.DownsampleMappingRules) > 0
		// Only downsample if the downsampler exists, and they either want to use the default mapping
		// rules, or they're trying to override the mapping rules and they've provided at least one
		// override to do so.
		shouldDownsample = downsamplerExists && (useDefaultMappingRules || downsampleOverride)
	)
	if shouldDownsample {
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
		if downsampleOverride {
			appenderOpts = downsample.SampleAppenderOptions{
				Override: true,
				OverrideRules: downsample.SamplesAppenderOverrideRules{
					MappingRules: overrides.DownsampleMappingRules,
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
	}

	return nil
}

func (d *downsamplerAndWriter) maybeWriteStorage(
	ctx context.Context,
	tags ident.TagIterator,
	tagOptions models.TagOptions,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	overrides WriteOptions,
) error {
	var (
		storageExists             = d.store != nil
		useDefaultStoragePolicies = !overrides.WriteOverride
	)

	if !storageExists {
		return nil
	}

	if storageExists && useDefaultStoragePolicies {
		// Duplicate so others can iterate in order.
		duplicate := tags.Duplicate()

		write := storage.NewWriteQuery(storage.WriteQueryOptions{
			Tags:       duplicate,
			TagOptions: tagOptions,
			Unit:       unit,
			Attributes: storage.Attributes{
				MetricsType: storage.UnaggregatedMetricsType,
			},
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

	for _, p := range overrides.WriteStoragePolicies {
		p := p // Capture for goroutine.

		wg.Add(1)
		d.workerPool.Go(func() {
			// Duplicate so others can iterate in order.
			duplicate := tags.Duplicate()

			write := storage.NewWriteQuery(storage.WriteQueryOptions{
				Tags:       duplicate,
				TagOptions: tagOptions,
				Unit:       unit,
				Attributes: storage.Attributes{
					// Assume all overridden storage policies are for aggregated namespaces.
					MetricsType: storage.AggregatedMetricsType,
					Resolution:  p.Resolution().Window,
					Retention:   p.Retention().Duration(),
				},
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
) BatchError {
	var (
		wg       = sync.WaitGroup{}
		multiErr xerrors.MultiError
		errLock  sync.Mutex
		addError = func(err error) {
			errLock.Lock()
			multiErr = multiErr.Add(err)
			errLock.Unlock()
		}
	)

	if d.store != nil {
		// Write unaggregated. Spin up all the background goroutines that make
		// network requests before we do the synchronous work of writing to the
		// downsampler.
		for iter.Next() {
			wg.Add(1)
			tags, datapoints, unit := iter.Current()
			d.workerPool.Go(func() {
				// Duplicate so others can iterate in order.
				duplicate := tags.Duplicate()

				write := storage.NewWriteQuery(storage.WriteQueryOptions{
					Tags:       duplicate,
					TagOptions: iter.TagOptions(),
					Unit:       unit,
					Attributes: storage.Attributes{
						MetricsType: storage.UnaggregatedMetricsType,
					},
				})
				write.AppendDatapoints(datapoints)
				if err := d.store.Write(ctx, write); err != nil {
					addError(err)
				}
				duplicate.Close()
				wg.Done()
			})
		}
	}

	// Iter does not need to be synchronized because even though we use it to spawn
	// many goroutines above, the iteration is always synchronous.
	resetErr := iter.Reset()
	if resetErr != nil {
		addError(resetErr)
	}

	if d.downsampler != nil && resetErr == nil {
		err := d.writeAggregatedBatch(iter)
		if err != nil {
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
) error {
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	defer appender.Finalize()

	var opts downsample.SampleAppenderOptions
	for iter.Next() {
		appender.Reset()
		tags, datapoints, _ := iter.Current()

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

	return iter.Error()
}

func (d *downsamplerAndWriter) Storage() storage.Storage {
	return d.store
}
