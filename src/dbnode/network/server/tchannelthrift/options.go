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
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

type options struct {
	instrumentOpts           instrument.Options
	blockMetadataV2Pool      BlockMetadataV2Pool
	blockMetadataV2SlicePool BlockMetadataV2SlicePool
	tagEncoderPool           serialize.TagEncoderPool
	tagDecoderPool           serialize.TagDecoderPool
}

// NewOptions creates new options
func NewOptions() Options {
	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(), pool.NewObjectPoolOptions(),
	)
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(), pool.NewObjectPoolOptions(),
	)

	tagEncoderPool.Init()
	tagDecoderPool.Init()

	return &options{
		instrumentOpts:           instrument.NewOptions(),
		blockMetadataV2Pool:      NewBlockMetadataV2Pool(nil),
		blockMetadataV2SlicePool: NewBlockMetadataV2SlicePool(nil, 0),
		tagEncoderPool:           tagEncoderPool,
		tagDecoderPool:           tagDecoderPool,
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
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
