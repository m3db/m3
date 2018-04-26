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

package index

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3ninx/doc"
)

// Batch represents a batch of documents that should be inserted into the index.
type Batch struct {
	Docs []doc.Document

	// If AllowPartialUpdates is true the index will continue to index documents in the batch
	// even if it encounters an error attempting to index a previous document in the batch.
	// If true, on the other hand, then any errors encountered indexing a document will cause
	// the entire batch to fail and none of the documents in the batch will be indexed.
	AllowPartialUpdates bool
}

// BatchOption is an option for a Batch.
type BatchOption interface {
	apply(Batch) Batch
}

// batchOptionFunc is an adaptor to allow the use of functions as BatchOptions.
type batchOptionFunc func(Batch) Batch

func (f batchOptionFunc) apply(b Batch) Batch {
	return f(b)
}

// AllowPartialUpdates permits an index to continue indexing documents in a batch even if
// it encountered an error inserting a prior document.
func AllowPartialUpdates() BatchOption {
	return batchOptionFunc(func(b Batch) Batch {
		b.AllowPartialUpdates = true
		return b
	})
}

// NewBatch returns a Batch of documents.
func NewBatch(docs []doc.Document, opts ...BatchOption) Batch {
	b := Batch{Docs: docs}

	for _, opt := range opts {
		b = opt.apply(b)
	}

	return b
}

// BatchPartialError indicates an error was encountered inserting some documents in a batch.
// It is not safe for concurrent use.
type BatchPartialError struct {
	errs []error
	idxs []int
}

// NewBatchPartialError returns a new BatchPartialError.
func NewBatchPartialError() *BatchPartialError {
	return &BatchPartialError{
		errs: make([]error, 0),
		idxs: make([]int, 0),
	}
}

func (e *BatchPartialError) Error() string {
	var b bytes.Buffer
	for i := range e.errs {
		b.WriteString(fmt.Sprintf("failed to insert document at index %v in batch: %v", e.idxs[i], e.errs[i]))
		if i != len(e.errs)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// Add adds an error to e. Any nil errors are ignored.
func (e *BatchPartialError) Add(err error, idx int) {
	if err == nil {
		return
	}
	e.errs = append(e.errs, err)
	e.idxs = append(e.idxs, idx)
}

// Indices returns the indices of the documents in the batch which were not indexed.
func (e *BatchPartialError) Indices() []int {
	return e.idxs
}

// IsEmpty returns a bool indicating whether e is empty or not.
func (e *BatchPartialError) IsEmpty() bool {
	return len(e.errs) == 0
}

// IsBatchPartialError returns a bool indicating whether err is a BatchPartialError or not.
func IsBatchPartialError(err error) bool {
	_, ok := err.(*BatchPartialError)
	return ok
}
