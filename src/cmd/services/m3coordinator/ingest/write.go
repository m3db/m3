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
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"
)

// DownsampleAndWriteIter is an interface that can be implemented to use
// the WriteBatch method.
type DownsampleAndWriteIter interface {
	Next() bool
	Current() (models.Tags, ts.Datapoints, xtime.Unit)
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
		downsamplingRules []downsample.MappingRule,
	) error

	// TODO(rartoul): Batch interface should also support downsampling rules.
	WriteBatch(
		ctx context.Context,
		iter DownsampleAndWriteIter,
	) error

	Storage() storage.Storage
}

// downsamplerAndWriter encapsulates the logic for writing data to the downsampler,
// as well as in unaggregated form to storage.
type downsamplerAndWriter struct {
	store       storage.Storage
	downsampler downsample.Downsampler
}

// NewDownsamplerAndWriter creates a new downsampler and writer.
func NewDownsamplerAndWriter(
	store storage.Storage,
	downsampler downsample.Downsampler,
) DownsamplerAndWriter {
	return &downsamplerAndWriter{
		store:       store,
		downsampler: downsampler,
	}
}

func (d *downsamplerAndWriter) Write(
	ctx context.Context,
	tags models.Tags,
	datapoints ts.Datapoints,
	unit xtime.Unit,
	downsamplingRules []downsample.MappingRule,
) error {
	if d.downsampler != nil {
		appender, err := d.downsampler.NewMetricsAppender()
		if err != nil {
			return err
		}

		for _, tag := range tags.Tags {
			appender.AddTag(tag.Name, tag.Value)
		}

		appenderOpts := downsample.SampleAppenderOptions{
			Override: true,
			OverrideRules: downsample.SamplesAppenderOverrideRules{
				MappingRules: downsamplingRules,
			},
		}
		samplesAppender, err := appender.SamplesAppender(appenderOpts)
		if err != nil {
			return err
		}

		for _, dp := range datapoints {
			err := samplesAppender.AppendGaugeSample(dp.Value)
			if err != nil {
				return err
			}
		}

		appender.Finalize()
	}

	if d.store != nil {
		return d.store.Write(ctx, &storage.WriteQuery{
			Tags:       tags,
			Datapoints: datapoints,
			Unit:       unit,
			Attributes: storage.Attributes{
				MetricsType: storage.UnaggregatedMetricsType,
			},
		})

	}

	return nil
}

func (d *downsamplerAndWriter) WriteBatch(
	ctx context.Context,
	iter DownsampleAndWriteIter,
) error {
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
			go func() {
				err := d.store.Write(ctx, &storage.WriteQuery{
					Tags:       tags,
					Datapoints: datapoints,
					Unit:       unit,
					Attributes: storage.Attributes{
						MetricsType: storage.UnaggregatedMetricsType,
					},
				})
				if err != nil {
					addError(err)
				}
				wg.Done()
			}()
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
	return multiErr.LastError()
}

func (d *downsamplerAndWriter) writeAggregatedBatch(
	iter DownsampleAndWriteIter,
) error {
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	var opts downsample.SampleAppenderOptions
	for iter.Next() {
		appender.Reset()
		tags, datapoints, _ := iter.Current()
		for _, tag := range tags.Tags {
			appender.AddTag(tag.Name, tag.Value)
		}

		samplesAppender, err := appender.SamplesAppender(opts)
		if err != nil {
			return err
		}

		for _, dp := range datapoints {
			err := samplesAppender.AppendGaugeSample(dp.Value)
			if err != nil {
				return err
			}
		}
	}
	appender.Finalize()

	if err := iter.Error(); err != nil {
		return err
	}

	return nil
}

func (d *downsamplerAndWriter) Storage() storage.Storage {
	return d.store
}
