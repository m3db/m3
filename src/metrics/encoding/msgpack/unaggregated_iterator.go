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
	"math"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"
)

const (
	defaultInitTimerValuesCapacity = 16
)

// unaggregatedIterator uses MessagePack to decode different types of unaggregated metrics.
// It is not thread-safe.
type unaggregatedIterator struct {
	iteratorBase

	largeFloatsSize     int
	largeFloatsPool     pool.FloatsPool
	iteratorPool        UnaggregatedIteratorPool
	ignoreHigherVersion bool

	closed             bool
	metric             unaggregated.MetricUnion
	policiesList       policy.PoliciesList
	id                 id.RawID
	timerValues        []float64
	cachedPolicies     [][]policy.Policy
	cachedPoliciesList policy.PoliciesList
}

// NewUnaggregatedIterator creates a new unaggregated iterator.
func NewUnaggregatedIterator(reader io.Reader, opts UnaggregatedIteratorOptions) UnaggregatedIterator {
	if opts == nil {
		opts = NewUnaggregatedIteratorOptions()
	}
	it := &unaggregatedIterator{
		iteratorBase:        newBaseIterator(reader, opts.ReaderBufferSize()),
		ignoreHigherVersion: opts.IgnoreHigherVersion(),
		largeFloatsSize:     opts.LargeFloatsSize(),
		largeFloatsPool:     opts.LargeFloatsPool(),
		iteratorPool:        opts.IteratorPool(),
		timerValues:         make([]float64, 0, defaultInitTimerValuesCapacity),
	}
	return it
}

func (it *unaggregatedIterator) Err() error { return it.err() }

func (it *unaggregatedIterator) Reset(reader io.Reader) {
	it.closed = false
	it.metric.Reset()
	it.reset(reader)
}

func (it *unaggregatedIterator) Metric() unaggregated.MetricUnion {
	return it.metric
}

func (it *unaggregatedIterator) PoliciesList() policy.PoliciesList {
	return it.policiesList
}

func (it *unaggregatedIterator) Next() bool {
	if it.err() != nil || it.closed {
		return false
	}

	// Reset the pointers in metric union to reduce GC sweep overhead.
	it.metric.BatchTimerVal = nil
	it.metric.TimerValPool = nil

	return it.decodeRootObject()
}

func (it *unaggregatedIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.reset(emptyReader)
	it.metric.Reset()
	it.policiesList = nil
	it.cachedPolicies = nil
	it.cachedPoliciesList = nil
	if it.iteratorPool != nil {
		it.iteratorPool.Put(it)
	}
}

func (it *unaggregatedIterator) decodeRootObject() bool {
	version := it.decodeVersion()
	if it.err() != nil {
		return false
	}
	// If the actual version is higher than supported version, we skip
	// the data for this metric and continue to the next.
	if version > unaggregatedVersion {
		if it.ignoreHigherVersion {
			it.skip(it.decodeNumObjectFields())
			return it.Next()
		}
		it.setErr(fmt.Errorf("received version %d is higher than supported version %d", version, unaggregatedVersion))
		return false
	}
	// Otherwise we proceed to decoding normally.
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(rootObjectType)
	if !ok {
		return false
	}
	objType := it.decodeObjectType()
	if it.err() != nil {
		return false
	}
	switch objType {
	case counterType, timerType, gaugeType:
		it.decodeMetric(objType)
	case counterWithPoliciesListType, batchTimerWithPoliciesListType, gaugeWithPoliciesListType:
		it.decodeMetricWithPoliciesList(objType)
	default:
		it.setErr(fmt.Errorf("unrecognized object type %v", objType))
	}
	it.skip(numActualFields - numExpectedFields)

	return it.err() == nil
}

func (it *unaggregatedIterator) decodeMetric(objType objectType) {
	switch objType {
	case counterType:
		it.decodeCounter()
	case timerType:
		it.decodeBatchTimer()
	case gaugeType:
		it.decodeGauge()
	default:
		it.setErr(fmt.Errorf("unrecognized metric type %v", objType))
	}
}

func (it *unaggregatedIterator) decodeMetricWithPoliciesList(objType objectType) {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(objType)
	if !ok {
		return
	}
	switch objType {
	case counterWithPoliciesListType:
		it.decodeCounter()
	case batchTimerWithPoliciesListType:
		it.decodeBatchTimer()
	case gaugeWithPoliciesListType:
		it.decodeGauge()
	default:
		it.setErr(fmt.Errorf("unrecognized metric with policies type %v", objType))
		return
	}
	it.decodePoliciesList()
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodeCounter() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(counterType)
	if !ok {
		return
	}
	it.metric.Type = metric.CounterType
	it.metric.ID = it.decodeID()
	it.metric.CounterVal = it.decodeVarint()
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodeBatchTimer() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(timerType)
	if !ok {
		return
	}
	it.metric.Type = metric.TimerType
	it.metric.ID = it.decodeID()
	var (
		timerValues []float64
		poolAlloc   = false
		numValues   = it.decodeArrayLen()
	)
	if cap(it.timerValues) >= numValues {
		it.timerValues = it.timerValues[:0]
		timerValues = it.timerValues
	} else if numValues <= it.largeFloatsSize {
		newCapcity := int(math.Max(float64(numValues), float64(cap(it.timerValues)*2)))
		if newCapcity > it.largeFloatsSize {
			newCapcity = it.largeFloatsSize
		}
		it.timerValues = make([]float64, 0, newCapcity)
		timerValues = it.timerValues
	} else {
		timerValues = it.largeFloatsPool.Get(numValues)
		poolAlloc = true
	}
	for i := 0; i < numValues; i++ {
		timerValues = append(timerValues, it.decodeFloat64())
	}
	it.metric.BatchTimerVal = timerValues
	if poolAlloc {
		it.metric.TimerValPool = it.largeFloatsPool
	}
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodeGauge() {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(gaugeType)
	if !ok {
		return
	}
	it.metric.Type = metric.GaugeType
	it.metric.ID = it.decodeID()
	it.metric.GaugeVal = it.decodeFloat64()
	it.skip(numActualFields - numExpectedFields)
}

func (it *unaggregatedIterator) decodePoliciesList() {
	numActualFields := it.decodeNumObjectFields()
	policiesListType := it.decodeObjectType()
	numExpectedFields, ok := it.checkExpectedNumFieldsForType(
		policiesListType,
		numActualFields,
	)
	if !ok {
		return
	}
	switch policiesListType {
	case defaultPoliciesListType:
		it.policiesList = policy.DefaultPoliciesList
	case customPoliciesListType:
		numStagedPolicies := it.decodeArrayLen()
		if cap(it.cachedPoliciesList) < numStagedPolicies {
			it.cachedPoliciesList = make(policy.PoliciesList, 0, numStagedPolicies)
		} else {
			for i := 0; i < len(it.cachedPoliciesList); i++ {
				it.cachedPoliciesList[i].Reset()
			}
			it.cachedPoliciesList = it.cachedPoliciesList[:0]
		}
		if len(it.cachedPolicies) < numStagedPolicies {
			it.cachedPolicies = make([][]policy.Policy, numStagedPolicies)
		}
		for policyIdx := 0; policyIdx < numStagedPolicies; policyIdx++ {
			decodedStagedPolicies := it.decodeStagedPolicies(policyIdx)
			it.cachedPoliciesList = append(it.cachedPoliciesList, decodedStagedPolicies)
		}
		it.policiesList = it.cachedPoliciesList
	default:
		it.setErr(fmt.Errorf("unrecognized policies list type: %v", policiesListType))
	}
	it.skip(numActualFields - numExpectedFields)
}

// decodeStagedPolicies decodes a staged policies using the cached policies
// to avoid memory reallocations and returns the decoded staged policies.
// If an error is encountered during decoding, it is stored in the iterator.
func (it *unaggregatedIterator) decodeStagedPolicies(policyIdx int) policy.StagedPolicies {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(stagedPoliciesType)
	if !ok {
		return policy.DefaultStagedPolicies
	}
	cutoverNanos := it.decodeVarint()
	tombstoned := it.decodeBool()
	numPolicies := it.decodeArrayLen()
	if cap(it.cachedPolicies[policyIdx]) < numPolicies {
		it.cachedPolicies[policyIdx] = make([]policy.Policy, 0, numPolicies)
	} else {
		it.cachedPolicies[policyIdx] = it.cachedPolicies[policyIdx][:0]
	}
	for i := 0; i < numPolicies; i++ {
		it.cachedPolicies[policyIdx] = append(it.cachedPolicies[policyIdx], it.decodePolicy())
	}
	stagedPolicies := policy.NewStagedPolicies(cutoverNanos, tombstoned, it.cachedPolicies[policyIdx])
	it.skip(numActualFields - numExpectedFields)
	return stagedPolicies
}

func (it *unaggregatedIterator) decodeID() id.RawID {
	idLen := it.decodeBytesLen()
	if it.err() != nil {
		return nil
	}
	// NB(xichen): DecodeBytesLen() returns -1 if the byte slice is nil.
	if idLen == -1 {
		it.id = it.id[:0]
		return it.id
	}
	if cap(it.id) < idLen {
		it.id = make([]byte, idLen)
	} else {
		it.id = it.id[:idLen]
	}
	if _, err := io.ReadFull(it.reader(), it.id); err != nil {
		it.setErr(err)
		return nil
	}
	return it.id
}
