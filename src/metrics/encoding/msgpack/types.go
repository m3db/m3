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

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3x/pool"
)

// Buffer is a byte buffer.
type Buffer interface {
	// Buffer returns the bytes buffer.
	Buffer() *bytes.Buffer

	// Bytes returns the buffered bytes.
	Bytes() []byte

	// Reset resets the buffer.
	Reset()

	// Close closes the buffer.
	Close()
}

// Encoder is an encoder.
type Encoder interface {
	// EncodeInt64 encodes an int64 value.
	EncodeInt64(value int64) error

	// EncodeBool encodes a boolean value.
	EncodeBool(value bool) error

	// EncodeFloat64 encodes a float64 value.
	EncodeFloat64(value float64) error

	// EncodeBytes encodes a byte slice.
	EncodeBytes(value []byte) error

	// EncodeBytesLen encodes the length of a byte slice.
	EncodeBytesLen(value int) error

	// EncodeArrayLen encodes the length of an array.
	EncodeArrayLen(value int) error
}

// BufferedEncoder is an encoder backed by byte buffers.
type BufferedEncoder interface {
	Buffer
	Encoder
}

// BufferedEncoderAlloc allocates a bufferer encoder.
type BufferedEncoderAlloc func() BufferedEncoder

// BufferedEncoderPool is a pool of buffered encoders.
type BufferedEncoderPool interface {
	// Init initializes the buffered encoder pool.
	Init(alloc BufferedEncoderAlloc)

	// Get returns a buffered encoder from the pool.
	Get() BufferedEncoder

	// Put puts a buffered encoder into the pool.
	Put(enc BufferedEncoder)
}

// BufferedEncoderPoolOptions provides options for buffered encoder pools.
type BufferedEncoderPoolOptions interface {
	// SetMaxCapacity sets the maximum capacity of buffers that can be returned to the pool.
	SetMaxCapacity(value int) BufferedEncoderPoolOptions

	// MaxBufferCapacity returns the maximum capacity of buffers that can be returned to the pool.
	MaxCapacity() int

	// SetObjectPoolOptions sets the object pool options.
	SetObjectPoolOptions(value pool.ObjectPoolOptions) BufferedEncoderPoolOptions

	// ObjectPoolOptions returns the object pool options.
	ObjectPoolOptions() pool.ObjectPoolOptions
}

// encoderBase is the base encoder interface.
type encoderBase interface {
	// Encoder returns the encoder.
	encoder() BufferedEncoder

	// err returns the error encountered during encoding, if any.
	err() error

	// reset resets the encoder.
	reset(encoder BufferedEncoder)

	// resetData resets the encoder data.
	resetData()

	// encodeStoragePolicy encodes a storage policy.
	encodeStoragePolicy(p policy.StoragePolicy)

	// encodePolicy encodes a policy.
	encodePolicy(p policy.Policy)

	// encodeVersion encodes a version.
	encodeVersion(version int)

	// encodeObjectType encodes an object type.
	encodeObjectType(objType objectType)

	// encodeNumObjectFields encodes the number of object fields.
	encodeNumObjectFields(numFields int)

	// encodeRawID encodes a raw ID.
	encodeRawID(id id.RawID)

	// encodeChunkedID encodes a chunked ID.
	encodeChunkedID(id id.ChunkedID)

	// encodeVarint encodes an integer value as varint.
	encodeVarint(value int64)

	// encodeBool encodes a boolean value.
	encodeBool(value bool)

	// encodeFloat64 encodes a float64 value.
	encodeFloat64(value float64)

	// encodeBytes encodes a byte slice.
	encodeBytes(value []byte)

	// encodeBytesLen encodes the length of a byte slice.
	encodeBytesLen(value int)

	// encodeArrayLen encodes the length of an array.
	encodeArrayLen(value int)
}

// iteratorBase is the base iterator interface.
type iteratorBase interface {
	// Reset resets the iterator.
	reset(reader io.Reader)

	// err returns the error encountered during decoding, if any.
	err() error

	// setErr sets the iterator error.
	setErr(err error)

	// reader returns the buffered reader.
	reader() bufReader

	// decodeStoragePolicy decodes a storage policy.
	decodeStoragePolicy() policy.StoragePolicy

	// decodePolicy decodes a policy.
	decodePolicy() policy.Policy

	// decodeVersion decodes a version.
	decodeVersion() int

	// decodeObjectType decodes an object type.
	decodeObjectType() objectType

	// decodeNumObjectFields decodes the number of object fields.
	decodeNumObjectFields() int

	// decodeRawID decodes a raw ID.
	decodeRawID() id.RawID

	// decodeVarint decodes a variable-width integer value.
	decodeVarint() int64

	// decodeBool decodes a boolean value.
	decodeBool() bool

	// decodeFloat64 decodes a float64 value.
	decodeFloat64() float64

	// decodeBytes decodes a byte slice.
	decodeBytes() []byte

	// decodeBytesLen decodes the length of a byte slice.
	decodeBytesLen() int

	// decodeArrayLen decodes the length of an array.
	decodeArrayLen() int

	// skip skips given number of fields if applicable.
	skip(numFields int)

	// checkNumFieldsForType decodes and compares the number of actual fields with
	// the number of expected fields for a given object type.
	checkNumFieldsForType(objType objectType) (int, int, bool)

	// checkExpectedNumFieldsForType compares the given number of actual fields with
	// the number of expected fields for a given object type.
	checkExpectedNumFieldsForType(objType objectType, numActualFields int) (int, bool)
}

// UnaggregatedEncoder is an encoder for encoding different types of unaggregated metrics.
type UnaggregatedEncoder interface {
	// EncodeCounter encodes a counter.
	EncodeCounter(c unaggregated.Counter) error

	// EncodeBatchTimer encodes a batch timer.
	EncodeBatchTimer(bt unaggregated.BatchTimer) error

	// EncodeGauge encodes a gauge.
	EncodeGauge(g unaggregated.Gauge) error

	// EncodeCounterWithPoliciesList encodes a counter with applicable policies list.
	EncodeCounterWithPoliciesList(cp unaggregated.CounterWithPoliciesList) error

	// EncodeBatchTimerWithPoliciesList encodes a batched timer with applicable policies list.
	EncodeBatchTimerWithPoliciesList(btp unaggregated.BatchTimerWithPoliciesList) error

	// EncodeGaugeWithPoliciesList encodes a gauge with applicable policies list.
	EncodeGaugeWithPoliciesList(gp unaggregated.GaugeWithPoliciesList) error

	// Encoder returns the encoder.
	Encoder() BufferedEncoder

	// Reset resets the encoder.
	Reset(encoder BufferedEncoder)
}

// UnaggregatedIterator is an iterator for decoding different types of unaggregated metrics.
type UnaggregatedIterator interface {
	// Next returns true if there are more items to decode.
	Next() bool

	// Metric returns the current metric.
	// The returned value remains valid until the next Next() call.
	Metric() unaggregated.MetricUnion

	// PoliciesList returns the current applicable policies list.
	// The returned value remains valid until the next Next() call.
	PoliciesList() policy.PoliciesList

	// Err returns the error encountered during decoding, if any.
	Err() error

	// Reset resets the iterator.
	Reset(reader io.Reader)

	// Close closes the iterator.
	Close()
}

// UnaggregatedIteratorOptions provide options for unaggregated iterators.
type UnaggregatedIteratorOptions interface {
	// SetIgnoreHigherVersion determines whether the iterator ignores messages
	// with higher-than-supported version.
	SetIgnoreHigherVersion(value bool) UnaggregatedIteratorOptions

	// IgnoreHigherVersion returns whether the iterator ignores messages with
	// higher-than-supported version.
	IgnoreHigherVersion() bool

	// SetReaderBufferSize sets the reader buffer size.
	SetReaderBufferSize(value int) UnaggregatedIteratorOptions

	// ReaderBufferSize returns the reader buffer size.
	ReaderBufferSize() int

	// SetLargeFloatsSize determines whether a float slice is considered a "large"
	// slice and therefore resort to the pool for allocating that slice.
	SetLargeFloatsSize(value int) UnaggregatedIteratorOptions

	// LargeFloatsSize returns whether a float slice is considered a "large"
	// slice and therefore resort to the pool for allocating that slice.
	LargeFloatsSize() int

	// SetLargeFloatsPool sets the large floats pool.
	SetLargeFloatsPool(value pool.FloatsPool) UnaggregatedIteratorOptions

	// LargeFloatsPool returns the large floats pool.
	LargeFloatsPool() pool.FloatsPool

	// SetIteratorPool sets the unaggregated iterator pool.
	SetIteratorPool(value UnaggregatedIteratorPool) UnaggregatedIteratorOptions

	// IteratorPool returns the unaggregated iterator pool.
	IteratorPool() UnaggregatedIteratorPool
}

// UnaggregatedIteratorAlloc allocates an unaggregated iterator.
type UnaggregatedIteratorAlloc func() UnaggregatedIterator

// UnaggregatedIteratorPool is a pool of unaggregated iterators.
type UnaggregatedIteratorPool interface {
	// Init initializes the unaggregated iterator pool.
	Init(alloc UnaggregatedIteratorAlloc)

	// Get returns an unaggregated iterator from the pool.
	Get() UnaggregatedIterator

	// Put puts an unaggregated iterator into the pool.
	Put(it UnaggregatedIterator)
}

// AggregatedEncoder is an encoder for encoding aggregated metrics.
type AggregatedEncoder interface {
	// EncodeChunkedMetricWithStoragePolicy encodes a chunked metric with an applicable storage policy.
	EncodeChunkedMetricWithStoragePolicy(cmp aggregated.ChunkedMetricWithStoragePolicy) error

	// EncodeChunkedMetricWithStoragePolicyAndEncodeTime encodes a chunked metric with
	// an applicable storage policy, alongside the time at which encoding happens.
	EncodeChunkedMetricWithStoragePolicyAndEncodeTime(
		cmp aggregated.ChunkedMetricWithStoragePolicy,
		encodedAtNanos int64,
	) error

	// Encoder returns the encoder.
	Encoder() BufferedEncoder

	// Reset resets the encoder.
	Reset(encoder BufferedEncoder)
}

// AggregatedIterator is an iterator for decoding aggregated metrics.
type AggregatedIterator interface {
	// Next returns true if there are more metrics to decode.
	Next() bool

	// Value returns the current raw metric, the corresponding policy, and timestamp at
	// which the metric and the policy were encoded if applicable.
	Value() (aggregated.RawMetric, policy.StoragePolicy, int64)

	// Err returns the error encountered during decoding, if any.
	Err() error

	// Reset resets the iterator.
	Reset(reader io.Reader)

	// Close closes the iterator.
	Close()
}

// AggregatedIteratorOptions provide options for aggregated iterators.
type AggregatedIteratorOptions interface {
	// SetIgnoreHigherVersion determines whether the iterator ignores messages
	// with higher-than-supported version.
	SetIgnoreHigherVersion(value bool) AggregatedIteratorOptions

	// IgnoreHigherVersion returns whether the iterator ignores messages with
	// higher-than-supported version.
	IgnoreHigherVersion() bool

	// SetReaderBufferSize sets the reader buffer size.
	SetReaderBufferSize(value int) AggregatedIteratorOptions

	// ReaderBufferSize returns the reader buffer size.
	ReaderBufferSize() int

	// SetIteratorPool sets the aggregated iterator pool.
	SetIteratorPool(value AggregatedIteratorPool) AggregatedIteratorOptions

	// IteratorPool returns the aggregated iterator pool.
	IteratorPool() AggregatedIteratorPool
}

// AggregatedIteratorAlloc allocates an aggregated iterator.
type AggregatedIteratorAlloc func() AggregatedIterator

// AggregatedIteratorPool is a pool of aggregated iterators.
type AggregatedIteratorPool interface {
	// Init initializes the aggregated iterator pool.
	Init(alloc AggregatedIteratorAlloc)

	// Get returns an aggregated iterator from the pool.
	Get() AggregatedIterator

	// Put puts an aggregated iterator into the pool.
	Put(it AggregatedIterator)
}
