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
	"fmt"
	"io"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/pool"
	xpool "github.com/m3db/m3x/pool"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyResolution policy.Resolution
	emptyRetention  policy.Retention
)

type decodeVarintFn func() int64
type decodeFloat64Fn func() float64
type decodeBytesFn func() []byte
type decodeArrayLenFn func() int

// rawIterator uses MessagePack to decode a stream of raw metrics. It is NOT thread-safe.
type rawIterator struct {
	decoder      *msgpack.Decoder         // internal decoder that does the actual decoding
	floatsPool   xpool.FloatsPool         // pool for float slices
	policiesPool pool.PoliciesPool        // pool for policies
	metric       metric.OneOf             // current raw metric
	policies     policy.VersionedPolicies // current policies
	err          error                    // error encountered during decoding

	decodeVarintFn   decodeVarintFn
	decodeFloat64Fn  decodeFloat64Fn
	decodeBytesFn    decodeBytesFn
	decodeArrayLenFn decodeArrayLenFn
}

// NewRawIterator creates a raw iterator
func NewRawIterator(reader io.Reader, opts RawIteratorOptions) (RawIterator, error) {
	if opts == nil {
		opts = NewRawIteratorOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	it := &rawIterator{
		decoder:      msgpack.NewDecoder(reader),
		floatsPool:   opts.FloatsPool(),
		policiesPool: opts.PoliciesPool(),
	}
	it.decodeVarintFn = it.decodeVarint
	it.decodeFloat64Fn = it.decodeFloat64
	it.decodeBytesFn = it.decodeBytes
	it.decodeArrayLenFn = it.decodeArrayLen

	return it, nil
}

func (it *rawIterator) Reset(reader io.Reader) {
	it.decoder.Reset(reader)
	it.err = nil
}

func (it *rawIterator) Value() (*metric.OneOf, policy.VersionedPolicies) {
	return &it.metric, it.policies
}

func (it *rawIterator) Err() error { return it.err }

func (it *rawIterator) Next() bool {
	if it.err != nil {
		return false
	}
	// Resetting the raw metric to avoid holding onto the float64 slices
	// in the metric field even though they may not be used
	it.metric.Reset()
	it.decodeVersion()
	it.decodeMetric()
	it.decodeVersionedPolicies()
	return it.err == nil
}

func (it *rawIterator) decodeMetric() {
	it.decodeType()
	it.decodeID()
	if it.err != nil {
		return
	}
	switch it.metric.Type {
	case metric.CounterType:
		it.decodeCounterValue()
	case metric.BatchTimerType:
		it.decodeBatchTimerValue()
	case metric.GaugeType:
		it.decodeGaugeValue()
	default:
		it.err = fmt.Errorf("unrecognized metric type %v", it.metric.Type)
	}
}

func (it *rawIterator) decodeCounterValue() {
	it.metric.CounterVal = int64(it.decodeVarintFn())
}

func (it *rawIterator) decodeBatchTimerValue() {
	numValues := it.decodeArrayLenFn()
	if it.err != nil {
		return
	}
	values := it.floatsPool.Get(numValues)
	for i := 0; i < numValues; i++ {
		values = append(values, it.decodeFloat64Fn())
	}
	it.metric.BatchTimerVal = values
}

func (it *rawIterator) decodeGaugeValue() {
	it.metric.GaugeVal = it.decodeFloat64Fn()
}

func (it *rawIterator) decodeVersionedPolicies() {
	version := int(it.decodeVarintFn())
	if it.err != nil {
		return
	}
	// NB(xichen): if the policy version is the default version, simply
	// return the default policies
	if version == policy.DefaultPolicyVersion {
		it.policies = policy.DefaultVersionedPolicies
		return
	}
	numPolicies := it.decodeArrayLenFn()
	if it.err != nil {
		return
	}
	policies := it.policiesPool.Get(numPolicies)
	for i := 0; i < numPolicies; i++ {
		policies = append(policies, it.decodePolicy())
	}
	it.policies = policy.VersionedPolicies{Version: version, Policies: policies}
}

func (it *rawIterator) decodeVersion() {
	version := int(it.decodeVarintFn())
	if it.err != nil {
		return
	}
	if version > supportedVersion {
		it.err = fmt.Errorf("decoded version %d is higher than supported version %d", version, supportedVersion)
	}
}

func (it *rawIterator) decodeType() {
	it.metric.Type = metric.Type(it.decodeVarintFn())
}

func (it *rawIterator) decodeID() {
	it.metric.ID = metric.IDType(it.decodeBytesFn())
}

func (it *rawIterator) decodePolicy() policy.Policy {
	resolution := it.decodeResolution()
	retention := it.decodeRetention()
	return policy.Policy{Resolution: resolution, Retention: retention}
}

func (it *rawIterator) decodeResolution() policy.Resolution {
	resolutionValue := policy.ResolutionValue(it.decodeVarintFn())
	resolution, err := resolutionValue.Resolution()
	if it.err != nil {
		return emptyResolution
	}
	it.err = err
	return resolution
}

func (it *rawIterator) decodeRetention() policy.Retention {
	retentionValue := policy.RetentionValue(it.decodeVarintFn())
	retention, err := retentionValue.Retention()
	if it.err != nil {
		return emptyRetention
	}
	it.err = err
	return retention
}

// NB(xichen): the underlying msgpack decoder implementation
// always decodes an int64 and looks at the actual decoded
// value to determine the width of the integer (a.k.a. varint
// decoding)
func (it *rawIterator) decodeVarint() int64 {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeInt64()
	it.err = err
	return value
}

func (it *rawIterator) decodeFloat64() float64 {
	if it.err != nil {
		return 0.0
	}
	value, err := it.decoder.DecodeFloat64()
	it.err = err
	return value
}

func (it *rawIterator) decodeBytes() []byte {
	if it.err != nil {
		return nil
	}
	value, err := it.decoder.DecodeBytes()
	it.err = err
	return value
}

func (it *rawIterator) decodeArrayLen() int {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeArrayLen()
	it.err = err
	return value
}
