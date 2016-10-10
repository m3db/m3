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

package ts

import (
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/pool"
)

type identifierPool struct {
	pool pool.ObjectPool
}

// NewIdentifierPool constructs a new Pool
func NewIdentifierPool(options pool.ObjectPoolOptions) IdentifierPool {
	p := pool.NewObjectPool(options)
	p.Init(func() interface{} { return &id{pool: p} })

	return &identifierPool{pool: p}
}

// GetBinaryID returns a new ID based on a binary blob
func (p *identifierPool) GetBinaryID(ctx context.Context, v []byte) ID {
	id := p.pool.Get().(*id)
	id.data = v
	ctx.RegisterCloser(id)

	return id
}

// GetStringID returns a new ID based on a string value
func (p *identifierPool) GetStringID(ctx context.Context, v string) ID {
	return p.GetBinaryID(ctx, []byte(v))
}
