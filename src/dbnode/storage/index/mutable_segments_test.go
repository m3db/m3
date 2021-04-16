// Copyright (c) 2021 Uber Technologies, Inc.
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

package index

import (
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPopulateCachedSearchesWorkerSafeCloserReuse(t *testing.T) {
	var all []*mockCloser
	defer func() {
		for _, c := range all {
			require.Equal(t, 1, c.closed)
		}
	}()

	w := newPopulateCachedSearchesWorker()
	for i := 0; i < 100; i++ {
		n := rand.Intn(64)
		for j := 0; j < n; j++ {
			closer := &mockCloser{}
			all = append(all, closer)
			w.addCloser(closer)
		}
		w.close()
	}
}

var _ io.Closer = (*mockCloser)(nil)

type mockCloser struct {
	closed int
}

func (c *mockCloser) Close() error {
	c.closed++
	return nil
}
