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

package mem

import (
	"github.com/m3db/m3ninx/index/segment"

	"github.com/m3db/m3x/instrument"
)

// Segment represents a memory backed index segment.
type Segment interface {
	segment.Segment
	segment.Readable
	segment.Writable

	// Open prepares the segment to accept reads/writes.
	Open() error

	// Close closes the segment.
	Close() error

	// Freeze makes the segment immutable. Any writes after this return error.
	Freeze() error
}

// Options is a collection of knobs for an in-memory segment.
type Options interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInsertQueueSize sets the insert queue size.
	SetInsertQueueSize(value int64) Options

	// InsertQueueSize returns the insert queue size.
	InsertQueueSize() int64

	// SetNumInsertWorkers sets the number of insert workers.
	SetNumInsertWorkers(value int) Options

	// NumInsertWorkers returns the number of insert workers.
	NumInsertWorkers() int
}
