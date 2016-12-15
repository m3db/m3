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

package msgpack

import (
	"bytes"
	"io"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/pool"
	xpool "github.com/m3db/m3x/pool"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	supportedVersion int = 1
)

// BufferedEncoder is an messagePack-based encoder backed by byte buffers
type BufferedEncoder struct {
	*msgpack.Encoder

	Buffer *bytes.Buffer
	pool   BufferedEncoderPool
}

// BufferedEncoderAlloc allocates a bufferer encoder
type BufferedEncoderAlloc func() BufferedEncoder

// BufferedEncoderPool is a pool of buffered encoders
type BufferedEncoderPool interface {
	// Init initializes the buffered encoder pool
	Init(alloc BufferedEncoderAlloc)

	// Get returns a buffered encoder from the pool
	Get() BufferedEncoder

	// Put puts a buffered encoder into the pool
	Put(enc BufferedEncoder)
}

// RawEncoder is a msgpack-based encoder for encoding raw metrics
type RawEncoder interface {
	// Encode encodes a raw metric with applicable policies
	Encode(m *metric.RawMetric, p policy.VersionedPolicies) error

	// Buffer returns the encoder buffer
	Encoder() BufferedEncoder

	// Reset resets the encoder
	Reset(buffer BufferedEncoder)
}

// RawIterator iterates over data stream and decodes raw metrics
type RawIterator interface {
	// Next returns true if there are more items to decode
	Next() bool

	// Value returns the current metric and applicable policies
	Value() (*metric.RawMetric, policy.VersionedPolicies)

	// Err returns the error encountered during decoding if any
	Err() error

	// Reset resets the iterator
	Reset(reader io.Reader)
}

// RawIteratorOptions provide options for raw iterators
type RawIteratorOptions interface {
	// SetFloatsPool sets the floats pool
	SetFloatsPool(value xpool.FloatsPool) RawIteratorOptions

	// FloatsPool returns the floats pool
	FloatsPool() xpool.FloatsPool

	// SetPoliciesPool sets the policies pool
	SetPoliciesPool(value pool.PoliciesPool) RawIteratorOptions

	// PoliciesPool returns the policies pool
	PoliciesPool() pool.PoliciesPool

	// Validate validates the options
	Validate() error
}
