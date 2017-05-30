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

type objectType int

const (
	// Current version for encoding unaggregated metrics.
	unaggregatedVersion int = 1

	// Current version for encoding aggregated metrics.
	aggregatedVersion int = 1

	// Current metric version.
	metricVersion int = 1
)

const (
	unknownType objectType = iota

	// Root object type.
	rootObjectType

	// Object types exposed to the encoder interface.
	counterWithPoliciesListType
	batchTimerWithPoliciesListType
	gaugeWithPoliciesListType
	rawMetricWithStoragePolicyType

	// Object types not exposed to the encoder interface.
	counterType
	batchTimerType
	gaugeType
	metricType
	defaultPoliciesListType
	customPoliciesListType
	stagedPoliciesType
	storagePolicyType
	knownResolutionType
	unknownResolutionType
	knownRetentionType
	unknownRetentionType
	defaultAggregationID
	shortAggregationID
	longAggregationID
	policyType

	// Total number of object types.
	numObjectTypes = iota
)

const (
	numRootObjectFields                 = 2
	numCounterWithPoliciesListFields    = 2
	numBatchTimerWithPoliciesListFields = 2
	numGaugeWithPoliciesListFields      = 2
	numRawMetricWithStoragePolicyFields = 2
	numCounterFields                    = 2
	numBatchTimerFields                 = 2
	numGaugeFields                      = 2
	numMetricFields                     = 3
	numDefaultStagedPoliciesListFields  = 1
	numCustomStagedPoliciesListFields   = 2
	numStagedPoliciesFields             = 3
	numStoragePolicyFields              = 2
	numKnownResolutionFields            = 2
	numUnknownResolutionFields          = 3
	numKnownRetentionFields             = 2
	numUnknownRetentionFields           = 2
	numDefaultAggregationIDFields       = 1
	numShortAggregationIDFields         = 2
	numLongAggregationIDFields          = 2
	numPolicyFields                     = 2
)

// NB(xichen): use a slice instead of a map to avoid lookup overhead.
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
	setNumFieldsForType(counterWithPoliciesListType, numCounterWithPoliciesListFields)
	setNumFieldsForType(batchTimerWithPoliciesListType, numBatchTimerWithPoliciesListFields)
	setNumFieldsForType(gaugeWithPoliciesListType, numGaugeWithPoliciesListFields)
	setNumFieldsForType(rawMetricWithStoragePolicyType, numRawMetricWithStoragePolicyFields)
	setNumFieldsForType(counterType, numCounterFields)
	setNumFieldsForType(batchTimerType, numBatchTimerFields)
	setNumFieldsForType(gaugeType, numGaugeFields)
	setNumFieldsForType(metricType, numMetricFields)
	setNumFieldsForType(defaultPoliciesListType, numDefaultStagedPoliciesListFields)
	setNumFieldsForType(customPoliciesListType, numCustomStagedPoliciesListFields)
	setNumFieldsForType(stagedPoliciesType, numStagedPoliciesFields)
	setNumFieldsForType(storagePolicyType, numStoragePolicyFields)
	setNumFieldsForType(knownResolutionType, numKnownResolutionFields)
	setNumFieldsForType(unknownResolutionType, numUnknownResolutionFields)
	setNumFieldsForType(knownRetentionType, numKnownRetentionFields)
	setNumFieldsForType(unknownRetentionType, numKnownRetentionFields)
	setNumFieldsForType(defaultAggregationID, numDefaultAggregationIDFields)
	setNumFieldsForType(shortAggregationID, numShortAggregationIDFields)
	setNumFieldsForType(longAggregationID, numLongAggregationIDFields)
	setNumFieldsForType(policyType, numPolicyFields)
}
