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

package client

import (
	"time"

	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/sampler"

	"go.uber.org/zap"
)

type fetchStatePool interface {
	Init()
	Get() *fetchState
	Put(*fetchState)
	MaybeLogHostError(hostErr maybeHostFetchError)
}

type fetchStatePoolImpl struct {
	pool pool.ObjectPool

	instrumentOpts      instrument.Options
	logger              *zap.Logger
	logHostErrorSampler *sampler.Sampler
}

func newFetchStatePool(
	opts pool.ObjectPoolOptions,
	logger *zap.Logger,
	logHostErrorSampler *sampler.Sampler,
) fetchStatePool {
	return &fetchStatePoolImpl{
		pool:                pool.NewObjectPool(opts),
		logger:              logger,
		logHostErrorSampler: logHostErrorSampler,
	}
}

func (p *fetchStatePoolImpl) Init() {
	p.pool.Init(func() interface{} {
		return newFetchState(p)
	})
}

func (p *fetchStatePoolImpl) Get() *fetchState {
	return p.pool.Get().(*fetchState)
}

func (p *fetchStatePoolImpl) Put(f *fetchState) {
	p.pool.Put(f)
}

func (p *fetchStatePoolImpl) MaybeLogHostError(hostErr maybeHostFetchError) {
	if hostErr.err == nil {
		// No error, this is an expected code path when host request doesn't
		// encounter an error.
		return
	}

	if !p.logHostErrorSampler.Sample() {
		return
	}

	p.logger.Warn("sampled error fetching from host (may not lead to consistency result error)",
		zap.Stringer("host", hostErr.host),
		zap.Duration("reqRespTime", hostErr.reqRespTime),
		zap.Error(hostErr.err))
}

type maybeHostFetchError struct {
	// Note: both these fields should be set always.
	host        topology.Host
	reqRespTime time.Duration

	// Error field is optionally set when there is actually an error.
	err error
}
