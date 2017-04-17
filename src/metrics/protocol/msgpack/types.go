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
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	xpool "github.com/m3db/m3x/pool"
)

// Buffer is a byte buffer
type Buffer interface {
	// Buffer returns the bytes buffer
	Buffer() *bytes.Buffer

	// Bytes returns the buffered bytes
	Bytes() []byte

	// Reset resets the buffer
	Reset()

	// Close closes the buffer
	Close()
}

// Encoder is an encoder
type Encoder interface {
	// EncodeTime encodes a time value
	EncodeTime(value time.Time) error

	// EncodeInt64 encodes an int64 value
	EncodeInt64(value int64) error

	// EncodeFloat64 encodes a float64 value
	EncodeFloat64(value float64) error

	// EncodeBytes encodes a byte slice
	EncodeBytes(value []byte) error

	// EncodeBytesLen encodes the length of a byte slice
	EncodeBytesLen(value int) error

	// EncodeArrayLen encodes the length of an array
	EncodeArrayLen(value int) error
}

// BufferedEncoder is an encoder backed by byte buffers
type BufferedEncoder interface {
	Buffer
	Encoder
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

// encoderBase is the base encoder interface
type encoderBase interface {
	// Encoder returns the encoder
	encoder() BufferedEncoder

	// err returns the error encountered during encoding, if any
	err() error

	// reset resets the encoder
	reset(encoder BufferedEncoder)

	// resetData resets the encoder data
	resetData()

	// encodePolicy encodes a policy
	encodePolicy(p policy.Policy)

	// encodeVersion encodes a version
	encodeVersion(version int)

	// encodeObjectType encodes an object type
	encodeObjectType(objType objectType)

	// encodeNumObjectFields encodes the number of object fields
	encodeNumObjectFields(numFields int)

	// encodeID encodes an ID
	encodeID(id metric.ID)

	// encodeChunkedID encodes a chunked ID
	encodeChunkedID(id metric.ChunkedID)

	// encodeTime encodes a time
	encodeTime(t time.Time)

	// encodeVarint encodes an integer value as varint
	encodeVarint(value int64)

	// encodeFloat64 encodes a float64 value
	encodeFloat64(value float64)

	// encodeBytes encodes a byte slice
	encodeBytes(value []byte)

	// encodeBytesLen encodes the length of a byte slice
	encodeBytesLen(value int)

	// encodeArrayLen encodes the length of an array
	encodeArrayLen(value int)
}

// iteratorBase is the base iterator interface
type iteratorBase interface {
	// Reset resets the iterator
	reset(reader io.Reader)

	// err returns the error encountered during decoding, if any
	err() error

	// setErr sets the iterator error
	setErr(err error)

	// decodePolicy decodes a policy
	decodePolicy() policy.Policy

	// decodeVersion decodes a version
	decodeVersion() int

	// decodeObjectType decodes an object type
	decodeObjectType() objectType

	// decodeNumObjectFields decodes the number of object fields
	decodeNumObjectFields() int

	// decodeID decodes an ID
	decodeID() metric.ID

	// decodeTime decodes a time
	decodeTime() time.Time

	// decodeVarint decodes a variable-width integer value
	decodeVarint() int64

	// decodeFloat64 decodes a float64 value
	decodeFloat64() float64

	// decodeBytes decodes a byte slice
	decodeBytes() []byte

	// decodeBytesLen decodes the length of a byte slice
	decodeBytesLen() int

	// decodeArrayLen decodes the length of an array
	decodeArrayLen() int

	// skip skips given number of fields if applicable
	skip(numFields int)

	// checkNumFieldsForType decodes and compares the number of actual fields with
	// the number of expected fields for a given object type
	checkNumFieldsForType(objType objectType) (int, int, bool)

	// checkNumFieldsForTypeWithActual compares the given number of actual fields with
	// the number of expected fields for a given object type
	checkNumFieldsForTypeWithActual(objType objectType, numActualFields int) (int, int, bool)
}

// UnaggregatedEncoder is an encoder for encoding different types of unaggregated metrics
type UnaggregatedEncoder interface {
	// EncodeCounterWithPolicies encodes a counter with applicable policies
	EncodeCounterWithPolicies(cp unaggregated.CounterWithPolicies) error

	// EncodeBatchTimerWithPolicies encodes a batched timer with applicable policies
	EncodeBatchTimerWithPolicies(btp unaggregated.BatchTimerWithPolicies) error

	// EncodeGaugeWithPolicies encodes a gauge with applicable policies
	EncodeGaugeWithPolicies(gp unaggregated.GaugeWithPolicies) error

	// Encoder returns the encoder
	Encoder() BufferedEncoder

	// Reset resets the encoder
	Reset(encoder BufferedEncoder)
}

// UnaggregatedIterator is an iterator for decoding different types of unaggregated metrics
type UnaggregatedIterator interface {
	// Next returns true if there are more items to decode
	Next() bool

	// Value returns the current metric and applicable policies
	Value() (unaggregated.MetricUnion, policy.VersionedPolicies)

	// Err returns the error encountered during decoding, if any
	Err() error

	// Reset resets the iterator
	Reset(reader io.Reader)

	// Close closes the iterator
	Close()
}

// UnaggregatedIteratorOptions provide options for unaggregated iterators
type UnaggregatedIteratorOptions interface {
	// SetIgnoreHigherVersion determines whether the iterator ignores messages
	// with higher-than-supported version
	SetIgnoreHigherVersion(value bool) UnaggregatedIteratorOptions

	// IgnoreHigherVersion returns whether the iterator ignores messages with
	// higher-than-supported version
	IgnoreHigherVersion() bool

	// SetFloatsPool sets the floats pool
	SetFloatsPool(value xpool.FloatsPool) UnaggregatedIteratorOptions

	// FloatsPool returns the floats pool
	FloatsPool() xpool.FloatsPool

	// SetPoliciesPool sets the policies pool
	SetPoliciesPool(value policy.PoliciesPool) UnaggregatedIteratorOptions

	// PoliciesPool returns the policies pool
	PoliciesPool() policy.PoliciesPool

	// SetIteratorPool sets the unaggregated iterator pool
	SetIteratorPool(value UnaggregatedIteratorPool) UnaggregatedIteratorOptions

	// IteratorPool returns the unaggregated iterator pool
	IteratorPool() UnaggregatedIteratorPool
}

// UnaggregatedIteratorAlloc allocates an unaggregated iterator
type UnaggregatedIteratorAlloc func() UnaggregatedIterator

// UnaggregatedIteratorPool is a pool of unaggregated iterators
type UnaggregatedIteratorPool interface {
	// Init initializes the unaggregated iterator pool
	Init(alloc UnaggregatedIteratorAlloc)

	// Get returns an unaggregated iterator from the pool
	Get() UnaggregatedIterator

	// Put puts an unaggregated iterator into the pool
	Put(it UnaggregatedIterator)
}

// AggregatedEncoder is an encoder for encoding aggregated metrics
type AggregatedEncoder interface {
	// EncodeMetricWithPolicy encodes a metric with an applicable policy
	EncodeMetricWithPolicy(mp aggregated.MetricWithPolicy) error

	// EncodeChunkedMetricWithPolicy encodes a chunked metric with an applicable policy
	EncodeChunkedMetricWithPolicy(cmp aggregated.ChunkedMetricWithPolicy) error

	// EncodeRawMetricWithPolicy encodes a raw metric with an applicable policy
	EncodeRawMetricWithPolicy(rp aggregated.RawMetricWithPolicy) error

	// Encoder returns the encoder
	Encoder() BufferedEncoder

	// Reset resets the encoder
	Reset(encoder BufferedEncoder)
}

// AggregatedIterator is an iterator for decoding aggregated metrics
type AggregatedIterator interface {
	// Next returns true if there are more metrics to decode
	Next() bool

	// Value returns the current raw metric and the applicable policy
	Value() (aggregated.RawMetric, policy.Policy)

	// Err returns the error encountered during decoding, if any
	Err() error

	// Reset resets the iterator
	Reset(reader io.Reader)

	// Close closes the iterator
	Close()
}

// AggregatedIteratorOptions provide options for aggregated iterators
type AggregatedIteratorOptions interface {
	// SetIgnoreHigherVersion determines whether the iterator ignores messages
	// with higher-than-supported version
	SetIgnoreHigherVersion(value bool) AggregatedIteratorOptions

	// IgnoreHigherVersion returns whether the iterator ignores messages with
	// higher-than-supported version
	IgnoreHigherVersion() bool

	// SetIteratorPool sets the aggregated iterator pool
	SetIteratorPool(value AggregatedIteratorPool) AggregatedIteratorOptions

	// IteratorPool returns the aggregated iterator pool
	IteratorPool() AggregatedIteratorPool
}

// AggregatedIteratorAlloc allocates an aggregated iterator
type AggregatedIteratorAlloc func() AggregatedIterator

// AggregatedIteratorPool is a pool of aggregated iterators
type AggregatedIteratorPool interface {
	// Init initializes the aggregated iterator pool
	Init(alloc AggregatedIteratorAlloc)

	// Get returns an aggregated iterator from the pool
	Get() AggregatedIterator

	// Put puts an aggregated iterator into the pool
	Put(it AggregatedIterator)
}
