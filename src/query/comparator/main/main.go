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

package main

import (
	"fmt"
	"net"
	"time"

	"github.com/m3db/m3/src/x/pool"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/tsdb/remote"
	"github.com/m3db/m3/src/x/instrument"
	"go.uber.org/zap"
)

func main() {
	var (
		iterPools   = pools.BuildIteratorPools()
		poolWrapper = pools.NewPoolsWrapper(iterPools)

		iOpts  = instrument.NewOptions()
		logger = iOpts.Logger()

		encoderPoolOpts = pool.NewObjectPoolOptions()
		encoderPool     = encoding.NewEncoderPool(encoderPoolOpts)
	)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Now(), nil, true, encoding.NewOptions())
	})

	querier := &querier{
		encoderPool:   encoderPool,
		iteratorPools: iterPools,
	}

	server := remote.NewGRPCServer(
		querier,
		models.QueryContextOptions{},
		poolWrapper,
		iOpts,
	)

	addr := "0.0.0.0:9000"
	logger.Info("Starting remote server", zap.String("address", addr))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("listener err: %s", err.Error())
		return
	}

	if err := server.Serve(listener); err != nil {
		fmt.Printf("serve err: %s", err.Error())
	}
}
