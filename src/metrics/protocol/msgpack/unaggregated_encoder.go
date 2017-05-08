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

// Various object-level encoding functions to facilitate testing.
type encodeRootObjectFn func(objType objectType)
type encodeCounterWithPoliciesListFn func(cp unaggregated.CounterWithPoliciesList)
type encodeBatchTimerWithPoliciesListFn func(btp unaggregated.BatchTimerWithPoliciesList)
type encodeGaugeWithPoliciesListFn func(gp unaggregated.GaugeWithPoliciesList)
type encodeCounterFn func(c unaggregated.Counter)
type encodeBatchTimerFn func(bt unaggregated.BatchTimer)
type encodeGaugeFn func(g unaggregated.Gauge)
type encodePoliciesListFn func(spl policy.PoliciesList)

// unaggregatedEncoder uses MessagePack for encoding different types of unaggregated metrics.
// It is not thread-safe.
type unaggregatedEncoder struct {
	encoderBase

	encodeRootObjectFn                 encodeRootObjectFn
	encodeCounterWithPoliciesListFn    encodeCounterWithPoliciesListFn
	encodeBatchTimerWithPoliciesListFn encodeBatchTimerWithPoliciesListFn
	encodeGaugeWithPoliciesListFn      encodeGaugeWithPoliciesListFn
	encodeCounterFn                    encodeCounterFn
	encodeBatchTimerFn                 encodeBatchTimerFn
	encodeGaugeFn                      encodeGaugeFn
	encodePoliciesListFn               encodePoliciesListFn
}

// NewUnaggregatedEncoder creates a new unaggregated encoder.
func NewUnaggregatedEncoder(encoder BufferedEncoder) UnaggregatedEncoder {
	enc := &unaggregatedEncoder{encoderBase: newBaseEncoder(encoder)}

	enc.encodeRootObjectFn = enc.encodeRootObject
	enc.encodeCounterWithPoliciesListFn = enc.encodeCounterWithPoliciesList
	enc.encodeBatchTimerWithPoliciesListFn = enc.encodeBatchTimerWithPoliciesList
	enc.encodeGaugeWithPoliciesListFn = enc.encodeGaugeWithPoliciesList
	enc.encodeCounterFn = enc.encodeCounter
	enc.encodeBatchTimerFn = enc.encodeBatchTimer
	enc.encodeGaugeFn = enc.encodeGauge
	enc.encodePoliciesListFn = enc.encodePoliciesList

	return enc
}

func (enc *unaggregatedEncoder) Encoder() BufferedEncoder      { return enc.encoder() }
func (enc *unaggregatedEncoder) Reset(encoder BufferedEncoder) { enc.reset(encoder) }

func (enc *unaggregatedEncoder) EncodeCounter(c unaggregated.Counter) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(counterType)
	enc.encodeCounterFn(c)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeBatchTimer(bt unaggregated.BatchTimer) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(batchTimerType)
	enc.encodeBatchTimerFn(bt)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeGauge(g unaggregated.Gauge) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(gaugeType)
	enc.encodeGaugeFn(g)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeCounterWithPoliciesList(cp unaggregated.CounterWithPoliciesList) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(counterWithPoliciesListType)
	enc.encodeCounterWithPoliciesListFn(cp)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeBatchTimerWithPoliciesList(btp unaggregated.BatchTimerWithPoliciesList) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(batchTimerWithPoliciesListType)
	enc.encodeBatchTimerWithPoliciesListFn(btp)
	return enc.err()
}

func (enc *unaggregatedEncoder) EncodeGaugeWithPoliciesList(gp unaggregated.GaugeWithPoliciesList) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(gaugeWithPoliciesListType)
	enc.encodeGaugeWithPoliciesListFn(gp)
	return enc.err()
}

func (enc *unaggregatedEncoder) encodeRootObject(objType objectType) {
	enc.encodeVersion(unaggregatedVersion)
	enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
	enc.encodeObjectType(objType)
}

func (enc *unaggregatedEncoder) encodeCounterWithPoliciesList(cp unaggregated.CounterWithPoliciesList) {
	enc.encodeNumObjectFields(numFieldsForType(counterWithPoliciesListType))
	enc.encodeCounterFn(cp.Counter)
	enc.encodePoliciesListFn(cp.PoliciesList)
}

func (enc *unaggregatedEncoder) encodeBatchTimerWithPoliciesList(btp unaggregated.BatchTimerWithPoliciesList) {
	enc.encodeNumObjectFields(numFieldsForType(batchTimerWithPoliciesListType))
	enc.encodeBatchTimerFn(btp.BatchTimer)
	enc.encodePoliciesListFn(btp.PoliciesList)
}

func (enc *unaggregatedEncoder) encodeGaugeWithPoliciesList(gp unaggregated.GaugeWithPoliciesList) {
	enc.encodeNumObjectFields(numFieldsForType(gaugeWithPoliciesListType))
	enc.encodeGaugeFn(gp.Gauge)
	enc.encodePoliciesListFn(gp.PoliciesList)
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

func (enc *unaggregatedEncoder) encodePoliciesList(pl policy.PoliciesList) {
	if pl.IsDefault() {
		enc.encodeNumObjectFields(numFieldsForType(defaultPoliciesListType))
		enc.encodeObjectType(defaultPoliciesListType)
		return
	}
	enc.encodeNumObjectFields(numFieldsForType(customPoliciesListType))
	enc.encodeObjectType(customPoliciesListType)
	numPolicies := len(pl)
	enc.encodeArrayLen(numPolicies)
	for i := 0; i < numPolicies; i++ {
		enc.encodeStagedPolicies(pl[i])
	}
}

func (enc *unaggregatedEncoder) encodeStagedPolicies(sp policy.StagedPolicies) {
	enc.encodeNumObjectFields(numFieldsForType(stagedPoliciesType))
	enc.encodeVarint(sp.CutoverNanos)
	enc.encodeBool(sp.Tombstoned)
	policies, _ := sp.Policies()
	enc.encodeArrayLen(len(policies))
	for _, policy := range policies {
		enc.encodePolicy(policy)
	}
}
