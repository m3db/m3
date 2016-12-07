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
	"crypto/md5"
	"sync"
	"testing"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstructorEquality(t *testing.T) {
	a := StringID("abc")
	b := BinaryID([]byte{'a', 'b', 'c'})

	require.Equal(t, a.String(), "abc")

	assert.True(t, a.Equal(b))
	assert.Equal(t, a.String(), b.String())
	assert.Equal(t, a.Data(), b.Data())
	assert.Equal(t, a.Hash(), b.Hash())
}

func TestPooling(t *testing.T) {
	p := NewIdentifierPool(nil, pool.NewObjectPoolOptions())
	ctx := context.NewContext()

	a := p.GetStringID(ctx, "abc")
	a.Hash()

	ctx.BlockingClose()

	require.Empty(t, a.Data())
}

func TestHashing(t *testing.T) {
	var (
		wg         sync.WaitGroup
		id         = StringID("abc")
		expected   = Hash(md5.Sum(id.Data()))
		numWorkers = 100
	)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(t, expected, id.Hash())
		}()
	}

	wg.Wait()
}

func TestCloning(t *testing.T) {
	a := StringID("abc")
	p := NewIdentifierPool(nil, pool.NewObjectPoolOptions())
	b := p.Clone(a)

	require.True(t, a.Equal(b))
}

func BenchmarkHashing(b *testing.B) {
	v := []byte{}

	for i := 0; i < b.N; i++ {
		id := BinaryID(v)
		id.Hash()
	}
}

func BenchmarkHashCaching(b *testing.B) {
	v := []byte{}

	for i := 0; i < b.N; i++ {
		id := BinaryID(v)
		id.Hash()
		id.Hash()
	}
}

func BenchmarkPooling(b *testing.B) {
	p := NewIdentifierPool(nil, pool.NewObjectPoolOptions())
	ctx := context.NewContext()

	v := []byte{'a', 'b', 'c'}

	for i := 0; i < b.N; i++ {
		id := p.GetBinaryID(ctx, v)
		id.Hash()
		id.Close()
	}
}
