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

	"github.com/m3db/m3db/src/m3ninx/index"
)

var (
	// ErrClosed is the error returned when attempting to perform operations on a
	// segment that has already been closed.
	ErrClosed = errors.New("segment has been closed")
)

// Segment is a sub-collection of documents within an index.
type Segment interface {
	// Size returns the number of documents within the Segment. It returns
	// 0 if the Segment has been closed.
	Size() int64

	// ContainsID returns a bool indicating if the Segment contains the provided ID.
	ContainsID(docID []byte) (bool, error)

	// Reader returns a point-in-time accessor to search the segment.
	Reader() (index.Reader, error)

	// Fields returns the list of known fields.
	Fields() ([][]byte, error)

	// Terms returns the list of known terms values for the given field.
	Terms(field []byte) ([][]byte, error)

	// Close closes the segment and releases any internal resources.
	Close() error
}

// MutableSegment is a segment which can be updated.
type MutableSegment interface {
	Segment
	index.Writer

	// Seal marks the Mutable Segment immutable.
	Seal() (Segment, error)

	// IsSealed returns true iff the segment is open and un-sealed.
	IsSealed() bool
}
