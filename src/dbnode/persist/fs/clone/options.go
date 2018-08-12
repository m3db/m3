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

package clone

import (
	"os"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3x/pool"
)

const (
	defaultBufferSize = 65536
	defaultFileMode   = os.FileMode(0666)
	defaultDirMode    = os.ModeDir | os.FileMode(0755)
)

type opts struct {
	pool       pool.CheckedBytesPool
	dOpts      msgpack.DecodingOptions
	bufferSize int
	fileMode   os.FileMode
	dirMode    os.FileMode
}

// NewOptions returns the new options
func NewOptions() Options {
	return &opts{
		pool:       nil,
		dOpts:      msgpack.NewDecodingOptions(),
		bufferSize: defaultBufferSize,
		fileMode:   defaultFileMode,
		dirMode:    defaultDirMode,
	}
}

func (o *opts) SetBytesPool(bytesPool pool.CheckedBytesPool) Options {
	o.pool = bytesPool
	return o
}

func (o *opts) BytesPool() pool.CheckedBytesPool {
	return o.pool
}

func (o *opts) SetDecodingOptions(decodingOpts msgpack.DecodingOptions) Options {
	o.dOpts = decodingOpts
	return o
}

func (o *opts) DecodingOptions() msgpack.DecodingOptions {
	return o.dOpts
}

func (o *opts) SetBufferSize(b int) Options {
	o.bufferSize = b
	return o
}

func (o *opts) BufferSize() int {
	return o.bufferSize
}

func (o *opts) SetFileMode(f os.FileMode) Options {
	o.fileMode = f
	return o
}

func (o *opts) FileMode() os.FileMode {
	return o.fileMode
}

func (o *opts) SetDirMode(d os.FileMode) Options {
	o.dirMode = d
	return o
}

func (o *opts) DirMode() os.FileMode {
	return o.dirMode
}
