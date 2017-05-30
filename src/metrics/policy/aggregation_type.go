// Copyright (c) 2017 Uber Technologies, Inc.
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

package policy

import (
	"fmt"
	"strings"

	"github.com/m3db/m3metrics/generated/proto/schema"
)

// Supported aggregation types.
const (
	Unknown AggregationType = iota
	Last
	Lower
	Upper
	Mean
	Median
	Count
	Sum
	SumSq
	Stdev
	P50
	P95
	P99
	P999
	P9999

	totalAggregationTypes = iota
)

const (
	// AggregationIDLen is the length of the AggregationID.
	// The AggregationIDLen will be 1 when totalAggregationTypes <= 64.
	AggregationIDLen = (totalAggregationTypes-1)/64 + 1

	aggregationTypesSeparator = ","
)

var (
	emptyStruct struct{}

	// DefaultAggregationTypes is a default list of aggregation types.
	DefaultAggregationTypes AggregationTypes

	// DefaultAggregationID is a default AggregationID.
	DefaultAggregationID AggregationID

	// ValidAggregationTypes is the list of all the valid aggregation types
	ValidAggregationTypes = map[AggregationType]struct{}{
		Last:   emptyStruct,
		Lower:  emptyStruct,
		Upper:  emptyStruct,
		Mean:   emptyStruct,
		Median: emptyStruct,
		Count:  emptyStruct,
		Sum:    emptyStruct,
		SumSq:  emptyStruct,
		Stdev:  emptyStruct,
		P50:    emptyStruct,
		P95:    emptyStruct,
		P99:    emptyStruct,
		P999:   emptyStruct,
		P9999:  emptyStruct,
	}

	aggregationTypeStringMap map[string]AggregationType
)

func init() {
	aggregationTypeStringMap = make(map[string]AggregationType, totalAggregationTypes)
	for aggType := range ValidAggregationTypes {
		aggregationTypeStringMap[aggType.String()] = aggType
	}
}

// AggregationType defines a custom aggregation function.
type AggregationType int

// NewAggregationTypeFromSchema creates an aggregation type from a schema.
func NewAggregationTypeFromSchema(input schema.AggregationType) (AggregationType, error) {
	aggType := AggregationType(input)
	if !aggType.IsValid() {
		return Unknown, fmt.Errorf("invalid aggregation type from schema: %s", input)
	}
	return aggType, nil
}

// IsValid checks if an AggregationType is valid.
func (a AggregationType) IsValid() bool {
	_, ok := ValidAggregationTypes[a]
	return ok
}

// IsValidForGauge if an AggregationType is valid for Gauge.
func (a AggregationType) IsValidForGauge() bool {
	switch a {
	case Last, Lower, Upper, Mean, Count, Sum, SumSq, Stdev:
		return true
	default:
		return false
	}
}

// IsValidForCounter if an AggregationType is valid for Counter.
func (a AggregationType) IsValidForCounter() bool {
	switch a {
	case Lower, Upper, Mean, Count, Sum, SumSq, Stdev:
		return true
	default:
		return false
	}
}

// IsValidForTimer if an AggregationType is valid for Timer.
func (a AggregationType) IsValidForTimer() bool {
	switch a {
	case Last:
		return false
	default:
		return true
	}
}

// ParseAggregationType parses an aggregation type.
func ParseAggregationType(str string) (AggregationType, error) {
	aggType, ok := aggregationTypeStringMap[str]
	if !ok {
		return Unknown, fmt.Errorf("invalid aggregation type: %s", str)
	}
	return aggType, nil
}

// AggregationTypes is a list of AggregationTypes.
type AggregationTypes []AggregationType

// NewAggregationTypesFromSchema creates a list of aggregation types from a schema.
func NewAggregationTypesFromSchema(input []schema.AggregationType) (AggregationTypes, error) {
	res := make([]AggregationType, len(input))
	for i, t := range input {
		aggType, err := NewAggregationTypeFromSchema(t)
		if err != nil {
			return DefaultAggregationTypes, err
		}
		res[i] = aggType
	}
	return res, nil
}

// IsDefault checks if the AggregationTypes is the default aggregation type.
func (aggTypes AggregationTypes) IsDefault() bool {
	return len(aggTypes) == 0
}

// String is for debugging.
func (aggTypes AggregationTypes) String() string {
	if len(aggTypes) == 0 {
		return ""
	}

	parts := make([]string, len(aggTypes))
	for i, aggType := range aggTypes {
		parts[i] = aggType.String()
	}
	return strings.Join(parts, aggregationTypesSeparator)
}

// IsValidForGauge checks if the list of aggregation types is valid for Gauge.
func (aggTypes AggregationTypes) IsValidForGauge() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForGauge() {
			return false
		}
	}
	return true
}

// IsValidForCounter checks if the list of aggregation types is valid for Counter.
func (aggTypes AggregationTypes) IsValidForCounter() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForCounter() {
			return false
		}
	}
	return true
}

// IsValidForTimer checks if the list of aggregation types is valid for Timer.
func (aggTypes AggregationTypes) IsValidForTimer() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForTimer() {
			return false
		}
	}
	return true
}

// ParseAggregationTypes parses a list of aggregation types in the form of type1,type2,type3.
func ParseAggregationTypes(str string) (AggregationTypes, error) {
	parts := strings.Split(str, aggregationTypesSeparator)
	res := make(AggregationTypes, len(parts))
	for i := range parts {
		aggType, err := ParseAggregationType(parts[i])
		if err != nil {
			return nil, err
		}
		res[i] = aggType
	}
	return res, nil
}

// AggregationID represents a compressed view of AggregationTypes.
type AggregationID [AggregationIDLen]uint64

// NewAggregationIDFromSchema creates an AggregationID from schema.
func NewAggregationIDFromSchema(input []schema.AggregationType) (AggregationID, error) {
	aggTypes, err := NewAggregationTypesFromSchema(input)
	if err != nil {
		return DefaultAggregationID, err
	}

	// TODO(cw): consider pooling these compressors,
	// this allocates one extra slice of length one per call.
	id, err := NewAggregationIDCompressor().Compress(aggTypes)
	if err != nil {
		return DefaultAggregationID, err
	}
	return id, nil
}

// IsDefault checks if the AggregationID is the default aggregation type.
func (id AggregationID) IsDefault() bool {
	return id == DefaultAggregationID
}

// Merge returns the result of merging another AggregationID, with an indicater whether
// any new aggregation type was found in the other AggregationID.
func (id AggregationID) Merge(other AggregationID) (AggregationID, bool) {
	var merged bool
	for i, code := range id {
		otherCode := other[i]
		if otherCode == 0 {
			continue
		}
		mergeResult := code | otherCode
		if code != mergeResult {
			merged = true
			id[i] = mergeResult
		}
	}
	return id, merged
}

// String for debugging.
func (id AggregationID) String() string {
	aggTypes, err := NewAggregationIDDecompressor().Decompress(id)
	if err != nil {
		return fmt.Sprintf("[invalid AggregationID: %v]", err)
	}

	return aggTypes.String()
}
