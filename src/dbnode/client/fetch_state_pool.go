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
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/sampler"

	"go.uber.org/zap"
)

type fetchStatePool interface {
	Init()
	Get() *fetchState
	Put(*fetchState)
	MaybeLogHostError(host topology.Host, err error)
}

type fetchStatePoolImpl struct {
	pool                pool.ObjectPool
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

func (p *fetchStatePoolImpl) MaybeLogHostError(host topology.Host, err error) {
	if err == nil {
		return
	}

	if !p.logHostErrorSampler.Sample() {
		return
	}

	p.logger.Warn("sampled error fetching from host (may not lead to consistency result error)",
		zap.Stringer("host", host),
		zap.Error(err))
}
