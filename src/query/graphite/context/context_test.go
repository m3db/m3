// Copyright (c) 2019 Uber Technologies, Inc.
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

package context

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWarnings(t *testing.T) {
	c := New()
	assert.Len(t, c.Warnings(), 0)
	// Check default case of no warnings returns unallocated slice
	// for performance savings in the regular case of serving a request
	// without any warnings
	assert.Equal(t, []error(nil), c.Warnings())

	err1 := errors.New("warning")
	err2 := errors.New("warning 2")
	err3 := errors.New("warning 3")

	c.AddWarning(err1)
	require.Len(t, c.Warnings(), 1)
	assert.Equal(t, err1, c.Warnings()[0])

	c.AddWarning(err2)
	require.Len(t, c.Warnings(), 2)
	assert.Equal(t, err1, c.Warnings()[0])
	assert.Equal(t, err2, c.Warnings()[1])

	c.ClearWarnings()
	assert.Len(t, c.Warnings(), 0)

	c.AddWarning(err3)
	require.Len(t, c.Warnings(), 1)
	assert.Equal(t, err3, c.Warnings()[0])

	c.AddWarning(err3)
	require.Len(t, c.Warnings(), 1)
	assert.Equal(t, err3, c.Warnings()[0])
}

func TestConfidence(t *testing.T) {
	c := New()
	assert.Equal(t, 1.0, c.Confidence())

	c.SetConfidence(0.5)
	c.SetConfidence(0.7)
	// make sure confidence doesn't change
	assert.Equal(t, 0.5, c.Confidence())
}

func TestConcurrentWrites(t *testing.T) {
	numWorkers := 10
	c := New()

	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < numWorkers; i++ {
		i := i
		go func() {
			for j := 0; j < 10; j++ {
				msg := fmt.Errorf("worker %v pass %v", i, j)
				c.AddWarning(msg)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	warnings := c.Warnings()
	assert.Len(t, warnings, 100)
}
