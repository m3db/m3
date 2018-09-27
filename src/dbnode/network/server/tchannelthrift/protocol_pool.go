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
package tchannelthrift

import (
	"github.com/m3db/m3x/pool"

	ubertchannelthrift "github.com/uber/tchannel-go/thrift"
)

type protocolPool struct {
	pool pool.ObjectPool
}

var _ ubertchannelthrift.ProtocolPool = &protocolPool{}

// NewProtocolPool returns a new protocol pool.
func NewProtocolPool(opts pool.ObjectPoolOptions, bytesPool pool.BytesPool) ubertchannelthrift.ProtocolPool {
	if bytesPool == nil {
		bytesPool = pool.NewBytesPool([]pool.Bucket{{Capacity: 256, Count: 256}}, opts)
		bytesPool.Init()
	}
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		transport := &ubertchannelthrift.OptimizedTRichTransport{}
		protocol := NewTBinaryProtocolTransport(transport, bytesPool)
		return &ubertchannelthrift.PooledProtocol{transport, protocol}
	})
	return protocolPool{p}
}

func (p protocolPool) Get() *ubertchannelthrift.PooledProtocol {
	return p.pool.Get().(*ubertchannelthrift.PooledProtocol)
}

func (p protocolPool) Release(value *ubertchannelthrift.PooledProtocol) {
	p.pool.Put(value)
}
