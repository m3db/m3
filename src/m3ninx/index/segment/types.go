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

package segment

import (
	"errors"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index"
)

var (
	// ErrClosed is the error returned when attempting to perform operations on a segment
	// that has already been closed.
	ErrClosed = errors.New("segment has been closed")
)

// Segment is a sub-collection of documents within an index.
type Segment interface {
	// Reader returns a point-in-time accessor to search the segment.
	Reader() (index.Reader, error)

	// Close closes the segment and releases any internal resources.
	Close() error
}

// MutableSegment is a segment which can be updated.
type MutableSegment interface {
	Segment

	// Insert inserts the given document into the segment. The document is guaranteed to be
	// searchable once the Insert method returns.
	Insert(d doc.Document) error

	// Seal marks the segment as immutable. After Seal is called no more documents can be
	// inserted into the segment.
	Seal() error
}
