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
	"errors"
	"io"

	"github.com/m3db/m3x/pool"

	tthrift "github.com/apache/thrift/lib/go/thrift"
)

// tBinaryProtocol wraps the underlying TBinaryProtocol within a struct to maintain
// an additional bytesPool reference.
type tBinaryProtocol struct {
	*tthrift.TBinaryProtocol
	bytesPool pool.BytesPool
}

func newTBinaryProtocolTransport(t tthrift.TTransport, p pool.BytesPool) tBinaryProtocol {
	return newTBinaryProtocol(t, p, false, true)
}

func newTBinaryProtocol(t tthrift.TTransport, pool pool.BytesPool, strictRead, strictWrite bool) tBinaryProtocol {
	if _, ok := t.(tthrift.TRichTransport); !ok {
		panic("unable to construct TBinaryProtocol from non TRichTransport")
	}
	return tBinaryProtocol{TBinaryProtocol: tthrift.NewTBinaryProtocol(t, strictRead, strictWrite), bytesPool: pool}
}

var invalidDataLength = tthrift.NewTProtocolExceptionWithType(tthrift.INVALID_DATA, errors.New("Invalid data length"))

func (p tBinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	if size < 0 {
		return nil, invalidDataLength
	}
	trans := p.Transport()
	if uint64(size) > trans.RemainingBytes() {
		return nil, invalidDataLength
	}

	isize := int(size)
	buf := p.bytesPool.Get(isize)[:isize]
	_, err := io.ReadFull(trans, buf)
	return buf, tthrift.NewTProtocolException(err)
}
