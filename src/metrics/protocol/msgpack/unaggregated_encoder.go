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
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

// various object-level encoding functions to facilitate testing
type encodeTopLevelFn func(objType objectType)
type encodeCounterFn func(c unaggregated.Counter)
type encodeBatchTimerFn func(bt unaggregated.BatchTimer)
type encodeGaugeFn func(g unaggregated.Gauge)
type encodePolicyFn func(p policy.Policy)
type encodeVersionedPoliciesFn func(vp policy.VersionedPolicies)

// various low-level encoding functions
type encodeVarintFn func(value int64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

// unaggregatedEncoder uses MessagePack for encoding different types of unaggregated metrics.
// It is not thread-safe.
type unaggregatedEncoder struct {
	encoderPool               BufferedEncoderPool       // pool for internal encoders
	encoder                   BufferedEncoder           // internal encoder that does the actual encoding
	err                       error                     // error encountered during encoding
	encodeTopLevelFn          encodeTopLevelFn          // top-level encoding function
	encodeCounterFn           encodeCounterFn           // counter encoding function
	encodeBatchTimerFn        encodeBatchTimerFn        // batch timer encoding function
	encodeGaugeFn             encodeGaugeFn             // gauge encoding function
	encodePolicyFn            encodePolicyFn            // policy encoding function
	encodeVersionedPoliciesFn encodeVersionedPoliciesFn // versioned policies encoding function
	encodeVarintFn            encodeVarintFn            // varint encoding function
	encodeFloat64Fn           encodeFloat64Fn           // float64 encoding function
	encodeBytesFn             encodeBytesFn             // byte slice encoding function
	encodeArrayLenFn          encodeArrayLenFn          // array length encoding function
}

// NewUnaggregatedEncoder creates a new unaggregated encoder
func NewUnaggregatedEncoder(encoder BufferedEncoder) (UnaggregatedEncoder, error) {
	enc := &unaggregatedEncoder{encoder: encoder}

	enc.encodeTopLevelFn = enc.encodeTopLevel
	enc.encodeCounterFn = enc.encodeCounter
	enc.encodeBatchTimerFn = enc.encodeBatchTimer
	enc.encodeGaugeFn = enc.encodeGauge
	enc.encodePolicyFn = enc.encodePolicy
	enc.encodeVersionedPoliciesFn = enc.encodeVersionedPolicies
	enc.encodeVarintFn = enc.encodeVarint
	enc.encodeFloat64Fn = enc.encodeFloat64
	enc.encodeBytesFn = enc.encodeBytes
	enc.encodeArrayLenFn = enc.encodeArrayLen

	return enc, nil
}

func (enc *unaggregatedEncoder) Encoder() BufferedEncoder {
	return enc.encoder
}

func (enc *unaggregatedEncoder) Reset(encoder BufferedEncoder) {
	enc.encoder = encoder
}

func (enc *unaggregatedEncoder) EncodeCounterWithPolicies(c unaggregated.Counter, vp policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeTopLevelFn(counterWithPoliciesType)
	enc.encodeCounterFn(c)
	enc.encodeVersionedPoliciesFn(vp)
	return enc.err
}

func (enc *unaggregatedEncoder) EncodeBatchTimerWithPolicies(bt unaggregated.BatchTimer, vp policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeTopLevelFn(batchTimerWithPoliciesType)
	enc.encodeBatchTimerFn(bt)
	enc.encodeVersionedPoliciesFn(vp)
	return enc.err
}

func (enc *unaggregatedEncoder) EncodeGaugeWithPolicies(g unaggregated.Gauge, vp policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeTopLevelFn(gaugeWithPoliciesType)
	enc.encodeGaugeFn(g)
	enc.encodeVersionedPoliciesFn(vp)
	return enc.err
}

func (enc *unaggregatedEncoder) encodeTopLevel(objType objectType) {
	enc.encodeVersion(supportedVersion)
	enc.encodeObjectType(objType)
	enc.encodeNumObjectFields(numFieldsForType(objType))
}

func (enc *unaggregatedEncoder) encodeCounter(c unaggregated.Counter) {
	enc.encodeNumObjectFields(numFieldsForType(counterType))
	enc.encodeID(c.ID)
	enc.encodeVarintFn(int64(c.Value))
}

func (enc *unaggregatedEncoder) encodeBatchTimer(bt unaggregated.BatchTimer) {
	enc.encodeNumObjectFields(numFieldsForType(batchTimerType))
	enc.encodeID(bt.ID)
	enc.encodeArrayLenFn(len(bt.Values))
	for _, v := range bt.Values {
		enc.encodeFloat64Fn(v)
	}
}

func (enc *unaggregatedEncoder) encodeGauge(g unaggregated.Gauge) {
	enc.encodeNumObjectFields(numFieldsForType(gaugeType))
	enc.encodeID(g.ID)
	enc.encodeFloat64Fn(g.Value)
}

func (enc *unaggregatedEncoder) encodePolicy(p policy.Policy) {
	enc.encodeNumObjectFields(numFieldsForType(policyType))
	enc.encodeResolution(p.Resolution)
	enc.encodeRetention(p.Retention)
}

func (enc *unaggregatedEncoder) encodeVersionedPolicies(vp policy.VersionedPolicies) {
	// NB(xichen): if this is a default policy, we only encode the policy version
	// and not the actual policies to optimize for the common case where the policies
	// are the default ones
	if vp.Version == policy.DefaultPolicyVersion {
		enc.encodeNumObjectFields(numFieldsForType(defaultVersionedPolicyType))
		enc.encodeVersion(vp.Version)
		return
	}

	// Otherwise fallback to encoding the entire object
	enc.encodeNumObjectFields(numFieldsForType(customVersionedPolicyType))
	enc.encodeVersion(vp.Version)
	enc.encodeArrayLenFn(len(vp.Policies))
	for _, policy := range vp.Policies {
		enc.encodePolicyFn(policy)
	}
}

func (enc *unaggregatedEncoder) encodeVersion(version int) {
	enc.encodeVarintFn(int64(version))
}

func (enc *unaggregatedEncoder) encodeObjectType(objType objectType) {
	enc.encodeVarintFn(int64(objType))
}

func (enc *unaggregatedEncoder) encodeNumObjectFields(numFields int) {
	enc.encodeArrayLenFn(numFields)
}

func (enc *unaggregatedEncoder) encodeID(id metric.ID) {
	enc.encodeBytesFn([]byte(id))
}

func (enc *unaggregatedEncoder) encodeResolution(resolution policy.Resolution) {
	if enc.err != nil {
		return
	}
	resolutionValue, err := policy.ValueFromResolution(resolution)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarintFn(int64(resolutionValue))
}

func (enc *unaggregatedEncoder) encodeRetention(retention policy.Retention) {
	if enc.err != nil {
		return
	}
	retentionValue, err := policy.ValueFromRetention(retention)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarintFn(int64(retentionValue))
}

// NB(xichen): the underlying msgpack encoder implementation
// always cast an integer value to an int64 and encodes integer
// values as varints, regardless of the actual integer type
func (enc *unaggregatedEncoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeInt64(value)
}

func (enc *unaggregatedEncoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeFloat64(value)
}

func (enc *unaggregatedEncoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeBytes(value)
}

func (enc *unaggregatedEncoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeArrayLen(value)
}
