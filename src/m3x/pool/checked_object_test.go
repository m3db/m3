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

package pool

import (
	"testing"

	"github.com/m3db/m3x/checked"

	"github.com/stretchr/testify/assert"
)

func TestCheckedObjectPool(t *testing.T) {
	type obj struct {
		checked.RefCount
		x int
	}

	opts := NewObjectPoolOptions().SetSize(1)

	p := NewCheckedObjectPool(opts)
	p.Init(func() checked.ReadWriteRef {
		return &obj{}
	})

	assert.Equal(t, 1, checkedObjectPoolLen(p))

	o := p.Get().(*obj)
	assert.Equal(t, 0, checkedObjectPoolLen(p))

	o.IncRef()
	o.IncWrites()
	o.x = 3
	o.DecWrites()
	o.DecRef()
	o.Finalize()

	assert.Equal(t, 1, checkedObjectPoolLen(p))

	o = p.Get().(*obj)
	assert.Equal(t, 0, checkedObjectPoolLen(p))

	o.IncRef()
	o.IncReads()
	assert.Equal(t, 3, o.x)
}

func TestCheckedObjectPoolNoOptions(t *testing.T) {
	p := NewCheckedObjectPool(nil)
	assert.NotNil(t, p)
}

func checkedObjectPoolLen(p CheckedObjectPool) int {
	return len(p.(*checkedObjectPool).pool.(*objectPool).values)
}
