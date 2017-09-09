// Copyright (c) 2017 Uber Technologies, Inc.
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

package pool_test

import (
	"fmt"

	"github.com/m3db/m3x/pool"
)

type exampleObject struct {
	a, b, c int64
}

func (o *exampleObject) reset() {
	o.a = 0
	o.b = 0
	o.c = 0
}

func ExampleObjectPool() {
	opts := pool.NewObjectPoolOptions()
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		// The Pool's Allocator should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation.
		return new(exampleObject)
	})

	// Get an exampleObject from the pool.
	o := p.Get().(*exampleObject)

	fmt.Printf("Retrieved struct should have default values: %+v", o)
	// Output: Retrieved struct should have default values: &{a:0 b:0 c:0}

	// Use the exampleObject.
	_ = o

	// Reset the exampleObject and return it to the pool.
	o.reset()
	p.Put(o)
}
