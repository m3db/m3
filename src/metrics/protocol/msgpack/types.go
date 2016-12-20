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

	"github.com/m3db/m3metrics/metric/unaggregated"
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

// UnaggregatedEncoder is an encoder for encoding different types of unaggregated metrics
type UnaggregatedEncoder interface {
	// EncodeCounterWithPolicies encodes a counter with applicable policies
	EncodeCounterWithPolicies(c unaggregated.Counter, vp policy.VersionedPolicies) error

	// EncodeBatchTimerWithPolicies encodes a batched timer with applicable policies
	EncodeBatchTimerWithPolicies(bt unaggregated.BatchTimer, vp policy.VersionedPolicies) error

	// EncodeGaugeWithPolicies encodes a gauge with applicable policies
	EncodeGaugeWithPolicies(g unaggregated.Gauge, vp policy.VersionedPolicies) error

	// Encoder returns the encoder
	Encoder() BufferedEncoder

	// Reset resets the encoder
	Reset(encoder BufferedEncoder)
}

// UnaggregatedIterator decodes different types of unaggregated metrics iteratively
type UnaggregatedIterator interface {
	// Next returns true if there are more items to decode
	Next() bool

	// Value returns the current metric and applicable policies
	Value() (*unaggregated.MetricUnion, policy.VersionedPolicies)

	// Err returns the error encountered during decoding if any
	Err() error

	// Reset resets the iterator
	Reset(reader io.Reader)
}

// UnaggregatedIteratorOptions provide options for unaggregated iterators
type UnaggregatedIteratorOptions interface {
	// SetFloatsPool sets the floats pool
	SetFloatsPool(value xpool.FloatsPool) UnaggregatedIteratorOptions

	// FloatsPool returns the floats pool
	FloatsPool() xpool.FloatsPool

	// SetPoliciesPool sets the policies pool
	SetPoliciesPool(value pool.PoliciesPool) UnaggregatedIteratorOptions

	// PoliciesPool returns the policies pool
	PoliciesPool() pool.PoliciesPool

	// Validate validates the options
	Validate() error
}
