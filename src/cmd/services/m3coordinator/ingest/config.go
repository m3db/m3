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

package ingest

import (
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	xsync "github.com/m3db/m3x/sync"
)

// Configuration configs the ingester.
type Configuration struct {
	WorkerPoolSize int                          `yaml:"workerPoolSize"`
	OpPool         pool.ObjectPoolConfiguration `yaml:"opPool"`
	Retry          retry.Configuration          `yaml:"retry"`
	LogSampleRate  float64                      `yaml:"logSampleRate" validate:"min=0.0,max=1.0"`
}

// NewIngester creates an ingester with an appender.
func (cfg Configuration) NewIngester(
	appender storage.Appender,
	instrumentOptions instrument.Options,
) (*Ingester, error) {
	opts, err := cfg.newOptions(appender, instrumentOptions)
	if err != nil {
		return nil, err
	}
	return NewIngester(opts), nil
}

func (cfg Configuration) newOptions(
	appender storage.Appender,
	instrumentOptions instrument.Options,
) (Options, error) {
	scope := instrumentOptions.MetricsScope().Tagged(
		map[string]string{"component": "ingester"},
	)
	workers, err := xsync.NewPooledWorkerPool(
		cfg.WorkerPoolSize,
		xsync.NewPooledWorkerPoolOptions().
			SetInstrumentOptions(instrumentOptions),
	)
	if err != nil {
		return Options{}, err
	}

	workers.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(),
		pool.NewObjectPoolOptions().
			SetInstrumentOptions(instrumentOptions.
				SetMetricsScope(instrumentOptions.MetricsScope().
					SubScope("tag-decoder-pool"))),
	)
	tagDecoderPool.Init()

	return Options{
		Appender:          appender,
		Workers:           workers,
		PoolOptions:       cfg.OpPool.NewObjectPoolOptions(instrumentOptions),
		TagDecoderPool:    tagDecoderPool,
		RetryOptions:      cfg.Retry.NewOptions(scope),
		InstrumentOptions: instrumentOptions,
		LogSampleRate:     cfg.LogSampleRate,
	}, nil
}
