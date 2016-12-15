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

package msgpack

import (
	"errors"

	"github.com/m3db/m3metrics/pool"
	xpool "github.com/m3db/m3x/pool"
)

var (
	errNoFloatsPool   = errors.New("no floats pool")
	errNoPoliciesPool = errors.New("no policies pool")
)

type multiTypedIteratorOptions struct {
	floatsPool   xpool.FloatsPool
	policiesPool pool.PoliciesPool
}

// NewMultiTypedIteratorOptions creates a new set of multi-typed iterator options
func NewMultiTypedIteratorOptions() MultiTypedIteratorOptions {
	floatsPool := xpool.NewFloatsPool(nil, nil)
	floatsPool.Init()

	policiesPool := pool.NewPoliciesPool(nil, nil)
	policiesPool.Init()

	return multiTypedIteratorOptions{
		floatsPool:   floatsPool,
		policiesPool: policiesPool,
	}
}

func (o multiTypedIteratorOptions) SetFloatsPool(value xpool.FloatsPool) MultiTypedIteratorOptions {
	opts := o
	opts.floatsPool = value
	return opts
}

func (o multiTypedIteratorOptions) FloatsPool() xpool.FloatsPool {
	return o.floatsPool
}

func (o multiTypedIteratorOptions) SetPoliciesPool(value pool.PoliciesPool) MultiTypedIteratorOptions {
	opts := o
	opts.policiesPool = value
	return opts
}

func (o multiTypedIteratorOptions) PoliciesPool() pool.PoliciesPool {
	return o.policiesPool
}

func (o multiTypedIteratorOptions) Validate() error {
	if o.floatsPool == nil {
		return errNoFloatsPool
	}
	if o.policiesPool == nil {
		return errNoPoliciesPool
	}
	return nil
}
