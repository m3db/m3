// Copyright (c) 2020 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package wide

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
)

func TestInvalidOptions(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	assert.Error(t, opts.Validate())

	opts = opts.SetReaderIteratorPool(encoding.NewMockReaderIteratorPool(ctrl))
	assert.NoError(t, opts.Validate())

	negativeCapacity := opts.SetFixedBufferCapacity(-1)
	assert.Error(t, negativeCapacity.Validate())

	negativeCount := opts.SetFixedBufferCount(-1)
	assert.Error(t, negativeCount.Validate())

	negativeTimeout := opts.SetFixedBufferTimeout(-1)
	assert.Error(t, negativeTimeout.Validate())
}

func TestOptions(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	pool := encoding.NewMockReaderIteratorPool(ctrl)
	opts := NewOptions().SetReaderIteratorPool(pool)
	assert.NoError(t, opts.Validate())
	assert.Equal(t, pool, opts.ReaderIteratorPool())
	assert.Equal(t, 10, opts.SetFixedBufferCapacity(10).FixedBufferCapacity())
	assert.Equal(t, 10, opts.SetFixedBufferCount(10).FixedBufferCount())
	assert.Equal(t, time.Second, opts.SetFixedBufferTimeout(time.Second).FixedBufferTimeout())
}
