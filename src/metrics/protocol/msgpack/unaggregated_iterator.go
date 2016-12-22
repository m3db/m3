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
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/pool"
	xpool "github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// unaggregatedIterator uses MessagePack to decode different types of unaggregated metrics.
// It is not thread-safe.
type unaggregatedIterator struct {
	decoder             *msgpack.Decoder         // internal decoder that does the actual decoding
	ignoreHigherVersion bool                     // whether we ignore messages with a higher-than-supported version
	floatsPool          xpool.FloatsPool         // pool for float slices
	policiesPool        pool.PoliciesPool        // pool for policies
	metric              unaggregated.MetricUnion // current metric
	versionedPolicies   policy.VersionedPolicies // current policies
	err                 error                    // error encountered during decoding
}

// NewUnaggregatedIterator creates a new unaggregated iterator
func NewUnaggregatedIterator(reader io.Reader, opts UnaggregatedIteratorOptions) (UnaggregatedIterator, error) {
	if opts == nil {
		opts = NewUnaggregatedIteratorOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	it := &unaggregatedIterator{
		decoder:             msgpack.NewDecoder(reader),
		ignoreHigherVersion: opts.IgnoreHigherVersion(),
		floatsPool:          opts.FloatsPool(),
		policiesPool:        opts.PoliciesPool(),
	}

	return it, nil
}

func (it *unaggregatedIterator) Reset(reader io.Reader) {
	it.decoder.Reset(reader)
	it.err = nil
}

func (it *unaggregatedIterator) Value() (unaggregated.MetricUnion, policy.VersionedPolicies) {
	return it.metric, it.versionedPolicies
}

func (it *unaggregatedIterator) Err() error { return it.err }

func (it *unaggregatedIterator) Next() bool {
	if it.err != nil {
		return false
	}

	// Resetting the metric to avoid holding onto the float64 slices
	// in the metric field even though they may not be used
	it.metric.Reset()

	return it.decodeRootObject()
}

func (it *unaggregatedIterator) decodeRootObject() bool {
	version := it.decodeVersion()
	if it.err != nil {
		return false
	}
	// If the actual version is higher than supported version, we skip
	// the data for this metric and continue to the next
	if version > unaggregatedVersion {
		if it.ignoreHigherVersion {
			it.skip(it.decodeNumObjectFields())
			return it.Next()
		}
		it.err = fmt.Errorf("received version %d is higher than supported version %d", version, unaggregatedVersion)
		return false
	}
	// Otherwise we proceed to decoding normally
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(rootObjectType)
	if !ok {
		return false
	}
	objType := it.decodeObjectType()
	if it.err != nil {
		return false
	}
	switch objType {
	case counterWithPoliciesType, batchTimerWithPoliciesType, gaugeWithPoliciesType:
		it.decodeMetricWithPolicies(objType)
	default:
		it.err = fmt.Errorf("unrecognized object type %v", objType)
	}
	it.skip(numActualFields - numExpectedFields)

	return it.err == nil
}

func (it *unaggregatedIterator) decodeMetricWithPolicies(objType objectType) {
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

func (it *unaggregatedIterator) decodeCounter() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(counterType)
	if !ok {
		return
	}
	it.metric.Type = unaggregated.CounterType
	it.metric.ID = it.decodeID()
	it.metric.CounterVal = int64(it.decodeVarint())
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodeBatchTimer() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(batchTimerType)
	if !ok {
		return
	}
	it.metric.Type = unaggregated.BatchTimerType
	it.metric.ID = it.decodeID()
	numValues := it.decodeArrayLen()
	values := it.floatsPool.Get(numValues)
	for i := 0; i < numValues; i++ {
		values = append(values, it.decodeFloat64())
	}
	it.metric.BatchTimerVal = values
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodeGauge() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(gaugeType)
	if !ok {
		return
	}
	it.metric.Type = unaggregated.GaugeType
	it.metric.ID = it.decodeID()
	it.metric.GaugeVal = it.decodeFloat64()
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodePolicy() policy.Policy {
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

func (it *unaggregatedIterator) decodeResolution() policy.Resolution {
	numActualFields := it.decodeNumObjectFields()
	resolutionType := it.decodeObjectType()
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		resolutionType,
		numActualFields,
	)
	if !ok {
		return policy.EmptyResolution
	}
	switch resolutionType {
	case knownResolutionType:
		resolutionValue := policy.ResolutionValue(it.decodeVarint())
		if !resolutionValue.IsValid() {
			it.err = fmt.Errorf("invalid resolution value %v", resolutionValue)
			return policy.EmptyResolution
		}
		it.skip(numActualFields - numExpectedFields)
		if it.err != nil {
			return policy.EmptyResolution
		}
		resolution, err := resolutionValue.Resolution()
		it.err = err
		return resolution
	case unknownResolutionType:
		window := time.Duration(it.decodeVarint())
		precision := xtime.Unit(it.decodeVarint())
		if it.err != nil {
			return policy.EmptyResolution
		}
		if !precision.IsValid() {
			it.err = fmt.Errorf("invalid precision %v", precision)
			return policy.EmptyResolution
		}
		it.skip(numActualFields - numExpectedFields)
		return policy.Resolution{Window: window, Precision: precision}
	default:
		it.err = fmt.Errorf("unrecognized resolution type %v", resolutionType)
		return policy.EmptyResolution
	}
}

func (it *unaggregatedIterator) decodeRetention() policy.Retention {
	numActualFields := it.decodeNumObjectFields()
	retentionType := it.decodeObjectType()
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		retentionType,
		numActualFields,
	)
	if !ok {
		return policy.EmptyRetention
	}
	switch retentionType {
	case knownRetentionType:
		retentionValue := policy.RetentionValue(it.decodeVarint())
		if !retentionValue.IsValid() {
			it.err = fmt.Errorf("invalid retention value %v", retentionValue)
			return policy.EmptyRetention
		}
		it.skip(numActualFields - numExpectedFields)
		if it.err != nil {
			return policy.EmptyRetention
		}
		retention, err := retentionValue.Retention()
		it.err = err
		return retention
	case unknownRetentionType:
		retention := policy.Retention(it.decodeVarint())
		it.skip(numActualFields - numExpectedFields)
		return retention
	default:
		it.err = fmt.Errorf("unrecognized retention type %v", retentionType)
		return policy.EmptyRetention
	}
}

func (it *unaggregatedIterator) decodeVersionedPolicies() {
	numActualFields := it.decodeNumObjectFields()
	versionedPoliciesType := it.decodeObjectType()
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		versionedPoliciesType,
		numActualFields,
	)
	if !ok {
		return
	}
	switch versionedPoliciesType {
	case defaultVersionedPoliciesType:
		it.versionedPolicies = policy.DefaultVersionedPolicies
		it.skip(numActualFields - numExpectedFields)
	case customVersionedPoliciesType:
		version := int(it.decodeVarint())
		numPolicies := it.decodeArrayLen()
		policies := it.policiesPool.Get(numPolicies)
		for i := 0; i < numPolicies; i++ {
			policies = append(policies, it.decodePolicy())
		}
		it.versionedPolicies = policy.VersionedPolicies{Version: version, Policies: policies}
		it.skip(numActualFields - numExpectedFields)
	default:
		it.err = fmt.Errorf("unrecognized versioned policies type: %v", versionedPoliciesType)
	}
}

// checkNumFieldsForType decodes and compares the number of actual fields with
// the number of expected fields for a given object type, returning true if
// the number of expected fields is no more than the number of actual fields
func (it *unaggregatedIterator) checkNumFieldsForType(objType objectType) (int, int, bool) {
	numActualFields := it.decodeNumObjectFields()
	return it.checkNumFieldsForTypeWithActual(objType, numActualFields)
}

func (it *unaggregatedIterator) checkNumFieldsForTypeWithActual(
	objType objectType,
	numActualFields int,
) (int, int, bool) {
	if it.err != nil {
		return 0, 0, false
	}
	numExpectedFields := numFieldsForType(objType)
	if numExpectedFields > numActualFields {
		it.err = fmt.Errorf("number of fields mismatch: expected %d actual %d", numExpectedFields, numActualFields)
		return 0, 0, false
	}
	return numExpectedFields, numActualFields, true
}

func (it *unaggregatedIterator) decodeVersion() int {
	return int(it.decodeVarint())
}

func (it *unaggregatedIterator) decodeObjectType() objectType {
	return objectType(it.decodeVarint())
}

func (it *unaggregatedIterator) decodeNumObjectFields() int {
	return int(it.decodeArrayLen())
}

func (it *unaggregatedIterator) decodeID() metric.ID {
	return metric.ID(it.decodeBytes())
}

// NB(xichen): the underlying msgpack decoder implementation
// always decodes an int64 and looks at the actual decoded
// value to determine the width of the integer (a.k.a. varint
// decoding)
func (it *unaggregatedIterator) decodeVarint() int64 {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeInt64()
	it.err = err
	return value
}

func (it *unaggregatedIterator) decodeFloat64() float64 {
	if it.err != nil {
		return 0.0
	}
	value, err := it.decoder.DecodeFloat64()
	it.err = err
	return value
}

func (it *unaggregatedIterator) decodeBytes() []byte {
	if it.err != nil {
		return nil
	}
	value, err := it.decoder.DecodeBytes()
	it.err = err
	return value
}

func (it *unaggregatedIterator) decodeArrayLen() int {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeArrayLen()
	it.err = err
	return value
}

func (it *unaggregatedIterator) skip(numFields int) {
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
