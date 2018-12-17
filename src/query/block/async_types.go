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
	"io"
)

// AsyncBuilder is an asynchronous block builder.
type AsyncBuilder interface {
	// AppendValue adds a value to a column at the given index.
	AppendValue(idx int, value float64)
	// AppendValues adds a slice of values to a column at the given index.
	AppendValues(idx int, values []float64)
	// Build builds the
	Build() Block
	// AddCols adds new columns to the builder.
	AddCols(num int)
	// Error returns any errors encountered during building.
	Error() error
}

// AsyncBlock represents a group of series accross a time bound with async
// iterators.
type AsyncBlock interface {
	io.Closer
	// AsyncStepIter returns an async step iterator.
	AsyncStepIter() (AsyncStepIter, error)
}

// StepCh is a channel that returns a step for asynchronously accessing steps.
// type StepCh <-chan (Step)

// IndexedStep combines a step with that step's intended index in the
// created block.
type IndexedStep struct {
	Idx int
	Step
}

// IndexedStepCh is a channel for asynchronously serving indexed steps.
// type IndexedStepCh <-chan (IndexedStep)

// AsyncStepIter iterates through a block vertically asynchronously.
type AsyncStepIter interface {
	Iterator
	StepMetaIter
	// Current returns the current step for the block.
	Current() <-chan (Step)
	// ValuesChannel returns a channel containing indexed steps for each value
	// in this iterator.
	ValuesChannel() <-chan (IndexedStep)
	// Err returns any errors encountered during iteration.
	Err() error
}
