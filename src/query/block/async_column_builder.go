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

package block

import (
	"sync"
)

// asyncColumnBlockBuilder builds a block optimized for column iteration
// in an asynchronous fashion.
type asyncColumnBlockBuilder struct {
	mu      sync.RWMutex
	err     error
	builder Builder
}

// NewAsyncColumnBlockBuilder creates a new asynchronous column block builder
// that wraps the given builder with async operations.
func NewAsyncColumnBlockBuilder(
	builder Builder,
) AsyncBuilder {
	return &asyncColumnBlockBuilder{
		builder: builder,
	}
}

func (cb *asyncColumnBlockBuilder) AppendValue(idx int, value float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.err != nil {
		return
	}

	cb.err = cb.builder.AppendValue(idx, value)
}

func (cb *asyncColumnBlockBuilder) AppendValues(
	idx int,
	values []float64,
) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.err != nil {
		return
	}

	cb.err = cb.builder.AppendValues(idx, values)
}

func (cb *asyncColumnBlockBuilder) AddCols(num int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.err != nil {
		return
	}

	cb.err = cb.builder.AddCols(num)
}

func (cb *asyncColumnBlockBuilder) Build() Block {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	if cb.err != nil {
		return nil
	}

	return cb.builder.Build()
}

func (cb *asyncColumnBlockBuilder) Error() error {
	cb.mu.RLock()
	err := cb.err
	cb.mu.RUnlock()
	return err
}
