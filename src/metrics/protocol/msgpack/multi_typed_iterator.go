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

// multiTypedIterator uses MessagePack to decode different types of metrics.
// It is not thread-safe.
type multiTypedIterator struct {
	decoder           *msgpack.Decoder         // internal decoder that does the actual decoding
	floatsPool        xpool.FloatsPool         // pool for float slices
	policiesPool      pool.PoliciesPool        // pool for policies
	metric            metric.OneOf             // current metric
	versionedPolicies policy.VersionedPolicies // current policies
	err               error                    // error encountered during decoding
}

// NewMultiTypedIterator creates a new multi-typed iterator
func NewMultiTypedIterator(reader io.Reader, opts MultiTypedIteratorOptions) (MultiTypedIterator, error) {
	if opts == nil {
		opts = NewMultiTypedIteratorOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	it := &multiTypedIterator{
		decoder:      msgpack.NewDecoder(reader),
		floatsPool:   opts.FloatsPool(),
		policiesPool: opts.PoliciesPool(),
	}

	return it, nil
}

func (it *multiTypedIterator) Reset(reader io.Reader) {
	it.decoder.Reset(reader)
	it.err = nil
}

func (it *multiTypedIterator) Value() (*metric.OneOf, policy.VersionedPolicies) {
	return &it.metric, it.versionedPolicies
}

func (it *multiTypedIterator) Err() error { return it.err }

func (it *multiTypedIterator) Next() bool {
	if it.err != nil {
		return false
	}

	// Resetting the metric to avoid holding onto the float64 slices
	// in the metric field even though they may not be used
	it.metric.Reset()
	version := it.decodeVersion()
	objType := it.decodeObjectType()
	if it.err != nil {
		return false
	}

	// If the actual version is higher than supported version, we skip
	// the data for this metric and continue to the next
	if version > supportedVersion {
		it.skip(it.decodeNumObjectFields())
		return it.Next()
	}

	// Otherwise we proceed to decoding normally
	switch objType {
	case counterWithPoliciesType, batchTimerWithPoliciesType, gaugeWithPoliciesType:
		it.decodeMetricWithPolicies(objType)
	default:
		it.err = fmt.Errorf("unrecognized object type %v", objType)
	}
	return it.err == nil
}

func (it *multiTypedIterator) decodeMetricWithPolicies(objType objectType) {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(objType)
	if !ok {
		return
	}
	switch objType {
	case counterWithPoliciesType:
		it.decodeCounter()
	case batchTimerWithPoliciesType:
		it.decodeBatchTimer()
	case gaugeWithPoliciesType:
		it.decodeGauge()
	default:
		it.err = fmt.Errorf("unrecognized metric with policies type %v", objType)
		return
	}
	it.decodeVersionedPolicies()
	it.skip(numActualFields - numExpectedFields)
}

func (it *multiTypedIterator) decodeCounter() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(counterType)
	if !ok {
		return
	}
	it.metric.Type = metric.CounterType
	it.metric.ID = it.decodeID()
	it.metric.CounterVal = int64(it.decodeVarint())
	it.skip(numActualFields - numExpectedFields)
}

func (it *multiTypedIterator) decodeBatchTimer() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(batchTimerType)
	if !ok {
		return
	}
	it.metric.Type = metric.BatchTimerType
	it.metric.ID = it.decodeID()
	numValues := it.decodeArrayLen()
	values := it.floatsPool.Get(numValues)
	for i := 0; i < numValues; i++ {
		values = append(values, it.decodeFloat64())
	}
	it.metric.BatchTimerVal = values
	it.skip(numActualFields - numExpectedFields)
}

func (it *multiTypedIterator) decodeGauge() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(gaugeType)
	if !ok {
		return
	}
	it.metric.Type = metric.GaugeType
	it.metric.ID = it.decodeID()
	it.metric.GaugeVal = it.decodeFloat64()
	it.skip(numActualFields - numExpectedFields)
}

func (it *multiTypedIterator) decodePolicy() policy.Policy {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(policyType)
	if !ok {
		return policy.Policy{}
	}
	resolution := it.decodeResolution()
	retention := it.decodeRetention()
	p := policy.Policy{Resolution: resolution, Retention: retention}
	it.skip(numActualFields - numExpectedFields)
	return p
}

func (it *multiTypedIterator) decodeVersionedPolicies() {
	numActualFields := it.decodeNumObjectFields()
	version := int(it.decodeVarint())
	if it.err != nil {
		return
	}

	// NB(xichen): if the policy version is the default version, simply
	// return the default policies
	if version == policy.DefaultPolicyVersion {
		numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
			defaultVersionedPolicyType,
			numActualFields,
		)
		if !ok {
			return
		}
		it.versionedPolicies = policy.DefaultVersionedPolicies
		it.skip(numActualFields - numExpectedFields)
		return
	}

	// Otherwise proceed to decoding the entire object
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		customVersionedPolicyType,
		numActualFields,
	)
	if !ok {
		return
	}
	numPolicies := it.decodeArrayLen()
	policies := it.policiesPool.Get(numPolicies)
	for i := 0; i < numPolicies; i++ {
		policies = append(policies, it.decodePolicy())
	}
	it.versionedPolicies = policy.VersionedPolicies{Version: version, Policies: policies}
	it.skip(numActualFields - numExpectedFields)
}

// checkNumFieldsForType decodes and compares the number of actual fields with
// the number of expected fields for a given object type, returning true if
// the number of expected fields is no more than the number of actual fields
func (it *multiTypedIterator) checkNumFieldsForType(objType objectType) (int, int, bool) {
	numActualFields := it.decodeNumObjectFields()
	return it.checkNumFieldsForTypeWithActual(objType, numActualFields)
}

func (it *multiTypedIterator) checkNumFieldsForTypeWithActual(
	objType objectType,
	numActualFields int,
) (int, int, bool) {
	numExpectedFields := numFieldsForType(objType)
	if it.err != nil {
		return 0, 0, false
	}
	if numExpectedFields > numActualFields {
		it.err = fmt.Errorf("number of fields mismatch: expected %d actual %d", numExpectedFields, numActualFields)
		return 0, 0, false
	}
	return numExpectedFields, numActualFields, true
}

func (it *multiTypedIterator) decodeVersion() int {
	return int(it.decodeVarint())
}

func (it *multiTypedIterator) decodeObjectType() objectType {
	return objectType(it.decodeVarint())
}

func (it *multiTypedIterator) decodeNumObjectFields() int {
	return int(it.decodeArrayLen())
}

func (it *multiTypedIterator) decodeID() metric.IDType {
	return metric.IDType(it.decodeBytes())
}

func (it *multiTypedIterator) decodeResolution() policy.Resolution {
	resolutionValue := policy.ResolutionValue(it.decodeVarint())
	resolution, err := resolutionValue.Resolution()
	if it.err != nil {
		return policy.EmptyResolution
	}
	it.err = err
	return resolution
}

func (it *multiTypedIterator) decodeRetention() policy.Retention {
	retentionValue := policy.RetentionValue(it.decodeVarint())
	retention, err := retentionValue.Retention()
	if it.err != nil {
		return policy.EmptyRetention
	}
	it.err = err
	return retention
}

// NB(xichen): the underlying msgpack decoder implementation
// always decodes an int64 and looks at the actual decoded
// value to determine the width of the integer (a.k.a. varint
// decoding)
func (it *multiTypedIterator) decodeVarint() int64 {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeInt64()
	it.err = err
	return value
}

func (it *multiTypedIterator) decodeFloat64() float64 {
	if it.err != nil {
		return 0.0
	}
	value, err := it.decoder.DecodeFloat64()
	it.err = err
	return value
}

func (it *multiTypedIterator) decodeBytes() []byte {
	if it.err != nil {
		return nil
	}
	value, err := it.decoder.DecodeBytes()
	it.err = err
	return value
}

func (it *multiTypedIterator) decodeArrayLen() int {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeArrayLen()
	it.err = err
	return value
}

func (it *multiTypedIterator) skip(numFields int) {
	if it.err != nil {
		return
	}
	if numFields < 0 {
		it.err = fmt.Errorf("number of fields to skip is %d", numFields)
		return
	}
	// Otherwise we skip any unexpected extra fields
	for i := 0; i < numFields; i++ {
		if err := it.decoder.Skip(); err != nil {
			it.err = err
			return
		}
	}
}
