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
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

// Various object-level encoding functions to facilitate testing
type encodeRootObjectFn func(objType objectType)
type encodeCounterWithPoliciesFn func(c unaggregated.Counter, vp policy.VersionedPolicies)
type encodeBatchTimerWithPoliciesFn func(bt unaggregated.BatchTimer, vp policy.VersionedPolicies)
type encodeGaugeWithPoliciesFn func(g unaggregated.Gauge, vp policy.VersionedPolicies)
type encodeCounterFn func(c unaggregated.Counter)
type encodeBatchTimerFn func(bt unaggregated.BatchTimer)
type encodeGaugeFn func(g unaggregated.Gauge)
type encodeVersionedPoliciesFn func(vp policy.VersionedPolicies)

// unaggregatedEncoder uses MessagePack for encoding different types of unaggregated metrics.
// It is not thread-safe.
type unaggregatedEncoder struct {
	encoderBase

	encodeRootObjectFn             encodeRootObjectFn             // root object encoding function
	encodeCounterWithPoliciesFn    encodeCounterWithPoliciesFn    // counter with policies encoding function
	encodeBatchTimerWithPoliciesFn encodeBatchTimerWithPoliciesFn // batch timer with policies encoding function
	encodeGaugeWithPoliciesFn      encodeGaugeWithPoliciesFn      // gauge with policies encoding function
	encodeCounterFn                encodeCounterFn                // counter encoding function
	encodeBatchTimerFn             encodeBatchTimerFn             // batch timer encoding function
	encodeGaugeFn                  encodeGaugeFn                  // gauge encoding function
	encodeVersionedPoliciesFn      encodeVersionedPoliciesFn      // versioned policies encoding function
}

// NewUnaggregatedEncoder creates a new unaggregated encoder
func NewUnaggregatedEncoder(encoder BufferedEncoder) UnaggregatedEncoder {
	enc := &unaggregatedEncoder{encoderBase: newBaseEncoder(encoder)}

	enc.encodeRootObjectFn = enc.encodeRootObject
	enc.encodeCounterWithPoliciesFn = enc.encodeCounterWithPolicies
	enc.encodeBatchTimerWithPoliciesFn = enc.encodeBatchTimerWithPolicies
	enc.encodeGaugeWithPoliciesFn = enc.encodeGaugeWithPolicies
	enc.encodeCounterFn = enc.encodeCounter
	enc.encodeBatchTimerFn = enc.encodeBatchTimer
	enc.encodeGaugeFn = enc.encodeGauge
	enc.encodeVersionedPoliciesFn = enc.encodeVersionedPolicies

	return enc
}

func (enc *unaggregatedEncoder) Encoder() BufferedEncoder      { return enc.encoder() }
func (enc *unaggregatedEncoder) Reset(encoder BufferedEncoder) { enc.reset(encoder) }

func (enc *unaggregatedEncoder) EncodeCounterWithPolicies(
	c unaggregated.Counter,
	vp policy.VersionedPolicies,
) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(counterWithPoliciesType)
	enc.encodeCounterWithPoliciesFn(c, vp)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeBatchTimerWithPolicies(
	bt unaggregated.BatchTimer,
	vp policy.VersionedPolicies,
) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(batchTimerWithPoliciesType)
	enc.encodeBatchTimerWithPoliciesFn(bt, vp)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeGaugeWithPolicies(
	g unaggregated.Gauge,
	vp policy.VersionedPolicies,
) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(gaugeWithPoliciesType)
	enc.encodeGaugeWithPoliciesFn(g, vp)
	return enc.err()
}

func (enc *unaggregatedEncoder) encodeRootObject(objType objectType) {
	enc.encodeVersion(unaggregatedVersion)
	enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
	enc.encodeObjectType(objType)
}

func (enc *unaggregatedEncoder) encodeCounterWithPolicies(
	c unaggregated.Counter,
	vp policy.VersionedPolicies,
) {
	enc.encodeNumObjectFields(numFieldsForType(counterWithPoliciesType))
	enc.encodeCounterFn(c)
	enc.encodeVersionedPoliciesFn(vp)
}

func (enc *unaggregatedEncoder) encodeBatchTimerWithPolicies(
	bt unaggregated.BatchTimer,
	vp policy.VersionedPolicies,
) {
	enc.encodeNumObjectFields(numFieldsForType(batchTimerWithPoliciesType))
	enc.encodeBatchTimerFn(bt)
	enc.encodeVersionedPoliciesFn(vp)
}

func (enc *unaggregatedEncoder) encodeGaugeWithPolicies(
	g unaggregated.Gauge,
	vp policy.VersionedPolicies,
) {
	enc.encodeNumObjectFields(numFieldsForType(gaugeWithPoliciesType))
	enc.encodeGaugeFn(g)
	enc.encodeVersionedPoliciesFn(vp)
}

func (enc *unaggregatedEncoder) encodeCounter(c unaggregated.Counter) {
	enc.encodeNumObjectFields(numFieldsForType(counterType))
	enc.encodeID(c.ID)
	enc.encodeVarint(int64(c.Value))
}

func (enc *unaggregatedEncoder) encodeBatchTimer(bt unaggregated.BatchTimer) {
	enc.encodeNumObjectFields(numFieldsForType(batchTimerType))
	enc.encodeID(bt.ID)
	enc.encodeArrayLen(len(bt.Values))
	for _, v := range bt.Values {
		enc.encodeFloat64(v)
	}
}

func (enc *unaggregatedEncoder) encodeGauge(g unaggregated.Gauge) {
	enc.encodeNumObjectFields(numFieldsForType(gaugeType))
	enc.encodeID(g.ID)
	enc.encodeFloat64(g.Value)
}

func (enc *unaggregatedEncoder) encodeVersionedPolicies(vp policy.VersionedPolicies) {
	// NB(xichen): if this is a default policy, we only encode the policy version
	// and not the actual policies to optimize for the common case where the policies
	// are the default ones
	if vp.Version == policy.DefaultPolicyVersion {
		enc.encodeNumObjectFields(numFieldsForType(defaultVersionedPoliciesType))
		enc.encodeObjectType(defaultVersionedPoliciesType)
		return
	}
	// Otherwise fallback to encoding the entire object
	enc.encodeNumObjectFields(numFieldsForType(customVersionedPoliciesType))
	enc.encodeObjectType(customVersionedPoliciesType)
	enc.encodeVersion(vp.Version)
	enc.encodeArrayLen(len(vp.Policies))
	for _, policy := range vp.Policies {
		enc.encodePolicy(policy)
	}
}
