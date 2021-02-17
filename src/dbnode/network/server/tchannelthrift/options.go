// Copyright (c) 2016 Uber Technologies, Inc.
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

package tchannelthrift

import (
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/storage/limits/permits"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
)

type options struct {
	clockOpts                   clock.Options
	instrumentOpts              instrument.Options
	topologyInitializer         topology.Initializer
	idPool                      ident.Pool
	blockMetadataV2Pool         BlockMetadataV2Pool
	blockMetadataV2SlicePool    BlockMetadataV2SlicePool
	tagEncoderPool              serialize.TagEncoderPool
	tagDecoderPool              serialize.TagDecoderPool
	checkedBytesWrapperPool     xpool.CheckedBytesWrapperPool
	maxOutstandingWriteRequests int
	maxOutstandingReadRequests  int
	queryLimits                 limits.QueryLimits
	permitsOptions              permits.Options
	seriesBlocksPerBatch        int
}

// NewOptions creates new options.
func NewOptions() Options {
	// Use a zero size pool by default, override from config.
	poolOptions := pool.NewObjectPoolOptions().
		SetSize(0)

	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()

	idPool := ident.NewPool(bytesPool, ident.PoolOptions{
		IDPoolOptions:           poolOptions,
		TagsPoolOptions:         poolOptions,
		TagsIteratorPoolOptions: poolOptions,
	})

	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		poolOptions)
	tagEncoderPool.Init()

	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		poolOptions)
	tagDecoderPool.Init()

	bytesWrapperPool := xpool.NewCheckedBytesWrapperPool(poolOptions)
	bytesWrapperPool.Init()

	return &options{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		idPool:                   idPool,
		blockMetadataV2Pool:      NewBlockMetadataV2Pool(nil),
		blockMetadataV2SlicePool: NewBlockMetadataV2SlicePool(nil, 0),
		tagEncoderPool:           tagEncoderPool,
		tagDecoderPool:           tagDecoderPool,
		checkedBytesWrapperPool:  bytesWrapperPool,
		queryLimits:              limits.NoOpQueryLimits(),
		permitsOptions:           permits.NewOptions(),
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetTopologyInitializer(value topology.Initializer) Options {
	opts := *o
	opts.topologyInitializer = value
	return &opts
}

func (o *options) TopologyInitializer() topology.Initializer {
	return o.topologyInitializer
}

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.idPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
	return o.idPool
}

func (o *options) SetBlockMetadataV2Pool(value BlockMetadataV2Pool) Options {
	opts := *o
	opts.blockMetadataV2Pool = value
	return &opts
}

func (o *options) BlockMetadataV2Pool() BlockMetadataV2Pool {
	return o.blockMetadataV2Pool
}

func (o *options) SetBlockMetadataV2SlicePool(value BlockMetadataV2SlicePool) Options {
	opts := *o
	opts.blockMetadataV2SlicePool = value
	return &opts
}

func (o *options) BlockMetadataV2SlicePool() BlockMetadataV2SlicePool {
	return o.blockMetadataV2SlicePool
}

func (o *options) SetTagEncoderPool(value serialize.TagEncoderPool) Options {
	opts := *o
	opts.tagEncoderPool = value
	return &opts
}

func (o *options) TagEncoderPool() serialize.TagEncoderPool {
	return o.tagEncoderPool
}

func (o *options) SetTagDecoderPool(value serialize.TagDecoderPool) Options {
	opts := *o
	opts.tagDecoderPool = value
	return &opts
}

func (o *options) TagDecoderPool() serialize.TagDecoderPool {
	return o.tagDecoderPool
}

func (o *options) SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options {
	opts := *o
	opts.checkedBytesWrapperPool = value
	return &opts
}

func (o *options) CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool {
	return o.checkedBytesWrapperPool
}

func (o *options) SetMaxOutstandingWriteRequests(value int) Options {
	opts := *o
	opts.maxOutstandingWriteRequests = value
	return &opts
}

func (o *options) MaxOutstandingWriteRequests() int {
	return o.maxOutstandingWriteRequests
}

func (o *options) SetMaxOutstandingReadRequests(value int) Options {
	opts := *o
	opts.maxOutstandingReadRequests = value
	return &opts
}

func (o *options) MaxOutstandingReadRequests() int {
	return o.maxOutstandingReadRequests
}

func (o *options) SetQueryLimits(value limits.QueryLimits) Options {
	opts := *o
	opts.queryLimits = value
	return &opts
}

func (o *options) QueryLimits() limits.QueryLimits {
	return o.queryLimits
}

func (o *options) SetPermitsOptions(value permits.Options) Options {
	opts := *o
	opts.permitsOptions = value
	return &opts
}

func (o *options) PermitsOptions() permits.Options {
	return o.permitsOptions
}

func (o *options) SetFetchTaggedSeriesBlocksPerBatch(value int) Options {
	opts := *o
	opts.seriesBlocksPerBatch = value
	return &opts
}

func (o *options) FetchTaggedSeriesBlocksPerBatch() int {
	return o.seriesBlocksPerBatch
}
