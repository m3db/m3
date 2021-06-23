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

package main

import (
	"net"
	"net/http"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/remote"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"

	"go.uber.org/zap"
)

var (
	iterPools = pools.BuildIteratorPools(
		pools.BuildIteratorPoolsOptions{})
	poolWrapper = pools.NewPoolsWrapper(iterPools)

	iOpts      = instrument.NewOptions()
	logger     = iOpts.Logger()
	tagOptions = models.NewTagOptions()

	encoderPoolOpts = pool.NewObjectPoolOptions()
	encoderPool     = encoding.NewEncoderPool(encoderPoolOpts)

	checkedBytesPool pool.CheckedBytesPool
	encodingOpts     encoding.Options
)

func init() {
	buckets := []pool.Bucket{{Capacity: 10, Count: 10}}
	newBackingBytesPool := func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	}

	checkedBytesPool = pool.NewCheckedBytesPool(buckets, nil, newBackingBytesPool)
	checkedBytesPool.Init()

	encodingOpts = encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetBytesPool(checkedBytesPool).
		SetMetrics(encoding.NewMetrics(iOpts.MetricsScope()))

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(0, nil, true, encodingOpts)
	})
}

func main() {
	opts := parser.Options{
		EncoderPool:       encoderPool,
		IteratorPools:     iterPools,
		TagOptions:        tagOptions,
		InstrumentOptions: iOpts,
	}

	seriesLoader := newHTTPSeriesLoadHandler(opts)

	querier, err := newQuerier(
		opts,
		seriesLoader,
		time.Hour*12,
		time.Second*15,
		5,
	)
	if err != nil {
		logger.Error("could not create querier", zap.Error(err))
		return
	}

	server := remote.NewGRPCServer(
		querier,
		models.QueryContextOptions{},
		poolWrapper,
		iOpts,
	)

	addr := "0.0.0.0:9000"
	logger.Info("starting remote server", zap.String("address", addr))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listener error", zap.Error(err))
		return
	}

	loaderAddr := "0.0.0.0:9001"
	go func() {
		if err := http.ListenAndServe(loaderAddr, seriesLoader); err != nil {
			logger.Error("series load handler failed", zap.Error(err))
		}
	}()

	if err := server.Serve(listener); err != nil {
		logger.Error("serve error", zap.Error(err))
	}
}
