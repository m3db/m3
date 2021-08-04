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

package prototest

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/namespace"
)

var (
	ProtoPools = newPools()
)

type Pools struct {
	EncodingOpt         encoding.Options
	EncoderPool         encoding.EncoderPool
	ReaderIterPool      encoding.ReaderIteratorPool
	MultiReaderIterPool encoding.MultiReaderIteratorPool
	BytesPool           pool.CheckedBytesPool
}

func newPools() Pools {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	testEncodingOptions := encoding.NewOptions().
		SetDefaultTimeUnit(xtime.Second).
		SetBytesPool(bytesPool)

	encoderPool := encoding.NewEncoderPool(nil)
	readerIterPool := encoding.NewReaderIteratorPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := testEncodingOptions.SetEncoderPool(encoderPool)

	encoderPool.Init(func() encoding.Encoder {
		return proto.NewEncoder(0, encodingOpts)
	})
	readerIterPool.Init(func(r xio.Reader64, descr namespace.SchemaDescr) encoding.ReaderIterator {
		return proto.NewIterator(r, descr, encodingOpts)
	})
	multiReaderIteratorPool.Init(func(r xio.Reader64, descr namespace.SchemaDescr) encoding.ReaderIterator {
		i := readerIterPool.Get()
		i.Reset(r, descr)
		return i
	})

	return Pools{
		EncodingOpt:         testEncodingOptions,
		BytesPool:           bytesPool,
		EncoderPool:         encoderPool,
		ReaderIterPool:      readerIterPool,
		MultiReaderIterPool: multiReaderIteratorPool,
	}
}
