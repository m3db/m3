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

package serialize

import (
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultCheckBytesWrapperPoolSize is the default size of the checked.Bytes
	// wrapper pool.
	defaultCheckBytesWrapperPoolSize = 4096
)

type decodeOpts struct {
	wrapperPool xpool.CheckedBytesWrapperPool
	limits      TagSerializationLimits
}

// NewTagDecoderOptions returns a new TagDecoderOptions.
func NewTagDecoderOptions() TagDecoderOptions {
	pool := xpool.NewCheckedBytesWrapperPool(
		pool.NewObjectPoolOptions().SetSize(defaultCheckBytesWrapperPoolSize))
	pool.Init()
	return &decodeOpts{
		wrapperPool: pool,
		limits:      NewTagSerializationLimits(),
	}
}

func (o *decodeOpts) SetCheckedBytesWrapperPool(v xpool.CheckedBytesWrapperPool) TagDecoderOptions {
	opts := *o
	opts.wrapperPool = v
	return &opts
}

func (o *decodeOpts) CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool {
	return o.wrapperPool
}

func (o *decodeOpts) SetTagSerializationLimits(v TagSerializationLimits) TagDecoderOptions {
	opts := *o
	opts.limits = v
	return &opts
}

func (o *decodeOpts) TagSerializationLimits() TagSerializationLimits {
	return o.limits
}
