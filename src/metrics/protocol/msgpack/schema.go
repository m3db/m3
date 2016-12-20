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

/*

The message-pack based encoder uses the following schema for encoding objects:

current encoder version
encoded object

When encoding an object, the encoder first writes the number of fields to be
encoded for the object, and then writes each object field. If an object field
is also an object, the encoder encodes that field recursively.

Backward-compatible changes (e.g., adding an additional field to the end of
the object) can be deployed or rolled back separately on the client-side and
the server-side. It is unnecessary to increase the version for
backward-compatible changes.

Backward-incompatible changes (e.g., removing a field or changing a field type)
must be deployed to the server-side first then to the client-side. It is REQUIRED
to increase the version for backward-incompatible changes.

*/

package msgpack

type objectType int

const (
	// Current version for encoding unaggregated metrics
	unaggregatedVersion int = 1
)

const (
	unknownType = iota

	// Root object type
	rootObjectType

	// Object types exposed to the encoder interface
	counterWithPoliciesType
	batchTimerWithPoliciesType
	gaugeWithPoliciesType

	// Object types not exposed to the encoder interface
	counterType
	batchTimerType
	gaugeType
	policyType
	knownResolutionType
	unknownResolutionType
	knownRetentionType
	unknownRetentionType
	defaultVersionedPoliciesType
	customVersionedPoliciesType

	// Total number of object types
	numObjectTypes = iota
)

const (
	numRootObjectFields             = 2
	numCounterWithPoliciesFields    = 2
	numBatchTimerWithPoliciesFields = 2
	numGaugeWithPoliciesFields      = 2
	numCounterFields                = 2
	numBatchTimerFields             = 2
	numGaugeFields                  = 2
	numPolicyFields                 = 2
	numKnownResolutionFields        = 2
	numUnknownResolutionFields      = 3
	numKnownRetentionFields         = 2
	numUnknownRetentionFields       = 2
	numDefaultVersionedPolicyFields = 1
	numCustomVersionedPolicyFields  = 3
)

var numObjectFields []int

func numFieldsForType(objType objectType) int {
	return numObjectFields[int(objType)-1]
}

func setNumFieldsForType(objType objectType, numFields int) {
	numObjectFields[int(objType)-1] = numFields
}

func init() {
	numObjectFields = make([]int, int(numObjectTypes))
	setNumFieldsForType(rootObjectType, numRootObjectFields)
	setNumFieldsForType(counterWithPoliciesType, numCounterWithPoliciesFields)
	setNumFieldsForType(batchTimerWithPoliciesType, numBatchTimerWithPoliciesFields)
	setNumFieldsForType(gaugeWithPoliciesType, numGaugeWithPoliciesFields)
	setNumFieldsForType(counterType, numCounterFields)
	setNumFieldsForType(batchTimerType, numBatchTimerFields)
	setNumFieldsForType(gaugeType, numGaugeFields)
	setNumFieldsForType(policyType, numPolicyFields)
	setNumFieldsForType(knownResolutionType, numKnownResolutionFields)
	setNumFieldsForType(unknownResolutionType, numUnknownResolutionFields)
	setNumFieldsForType(knownRetentionType, numKnownRetentionFields)
	setNumFieldsForType(unknownRetentionType, numKnownRetentionFields)
	setNumFieldsForType(defaultVersionedPoliciesType, numDefaultVersionedPolicyFields)
	setNumFieldsForType(customVersionedPoliciesType, numCustomVersionedPolicyFields)

}
