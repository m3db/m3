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

package server

import (
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	defaultWorkerPoolCount = 4096
	defaultWorkerPoolSize  = 20
)

func buildWorkerPools(
	cfg config.Configuration,
	logger *zap.Logger,
	scope tally.Scope,
) (pool.ObjectPool, instrument.Options) {
	workerPoolCount := cfg.DecompressWorkerPoolCount
	if workerPoolCount == 0 {
		workerPoolCount = defaultWorkerPoolCount
	}

	workerPoolSize := cfg.DecompressWorkerPoolSize
	if workerPoolSize == 0 {
		workerPoolSize = defaultWorkerPoolSize
	}

	instrumentOptions := instrument.NewOptions().
		SetZapLogger(logger).
		SetMetricsScope(scope.SubScope("series-decompression-pool"))

	poolOptions := pool.NewObjectPoolOptions().
		SetSize(workerPoolCount).
		SetInstrumentOptions(instrumentOptions)

	objectPool := pool.NewObjectPool(poolOptions)
	objectPool.Init(func() interface{} {
		workerPool := xsync.NewWorkerPool(workerPoolSize)
		workerPool.Init()
		return workerPool
	})

	return objectPool, instrumentOptions
}
