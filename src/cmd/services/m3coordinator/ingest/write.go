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

package write

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
	) error

	WriteBatch(
		ctx context.Context,
		iter DownsampleAndWriteIter,
	) error
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
) error {
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	for _, tag := range tags.Tags {
		appender.AddTag(tag.Name, tag.Value)
	}

	samplesAppender, err := appender.SamplesAppender()
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

	return d.store.Write(ctx, &storage.WriteQuery{
		Tags:       tags,
		Datapoints: datapoints,
		Unit:       unit,
		Attributes: storage.Attributes{
			MetricsType: storage.UnaggregatedMetricsType,
		},
	})
}

func (d *downsamplerAndWriter) WriteBatch(
	ctx context.Context,
	iter DownsampleAndWriteIter,
) error {
	// Write aggregated.
	appender, err := d.downsampler.NewMetricsAppender()
	if err != nil {
		return err
	}

	for iter.Next() {
		appender.Reset()
		tags, datapoints, _ := iter.Current()
		for _, tag := range tags.Tags {
			appender.AddTag(tag.Name, tag.Value)
		}

		samplesAppender, err := appender.SamplesAppender()
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
	if err := iter.Reset(); err != nil {
		return err
	}

	// Write unaggregated.
	var (
		wg       = &sync.WaitGroup{}
		errLock  sync.Mutex
		multiErr xerrors.MultiError
	)
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
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return multiErr.LastError()
}
