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
	"github.com/m3db/m3x/pool"
)

// Supported aggregation types.
const (
	Unknown AggregationType = iota
	Last
	Min
	Max
	Mean
	Median
	Count
	Sum
	SumSq
	Stdev
	P10
	P20
	P30
	P40
	P50
	P60
	P70
	P80
	P90
	P95
	P99
	P999
	P9999

	nextAggregationTypeID = iota
)

const (
	// MaxAggregationTypeID is the largest id of all the valid aggregation types.
	// NB(cw) MaxAggregationTypeID is guaranteed to be greater or equal
	// to len(ValidAggregationTypes).
	// Iff ids of all the valid aggregation types are consecutive,
	// MaxAggregationTypeID == len(ValidAggregationTypes).
	MaxAggregationTypeID = nextAggregationTypeID - 1

	// AggregationIDLen is the length of the AggregationID.
	// The AggregationIDLen will be 1 when MaxAggregationTypeID <= 63.
	AggregationIDLen = (MaxAggregationTypeID)/64 + 1

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
		Min:    emptyStruct,
		Max:    emptyStruct,
		Mean:   emptyStruct,
		Median: emptyStruct,
		Count:  emptyStruct,
		Sum:    emptyStruct,
		SumSq:  emptyStruct,
		Stdev:  emptyStruct,
		P10:    emptyStruct,
		P20:    emptyStruct,
		P30:    emptyStruct,
		P40:    emptyStruct,
		P50:    emptyStruct,
		P60:    emptyStruct,
		P70:    emptyStruct,
		P80:    emptyStruct,
		P90:    emptyStruct,
		P95:    emptyStruct,
		P99:    emptyStruct,
		P999:   emptyStruct,
		P9999:  emptyStruct,
	}

	aggregationTypeStringMap map[string]AggregationType
)

func init() {
	aggregationTypeStringMap = make(map[string]AggregationType, MaxAggregationTypeID)
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

// ID returns the id of the AggregationType.
func (a AggregationType) ID() int {
	return int(a)
}

// IsValid checks if an AggregationType is valid.
func (a AggregationType) IsValid() bool {
	_, ok := ValidAggregationTypes[a]
	return ok
}

// IsValidForGauge if an AggregationType is valid for Gauge.
func (a AggregationType) IsValidForGauge() bool {
	switch a {
	case Last, Min, Max, Mean, Count, Sum, SumSq, Stdev:
		return true
	default:
		return false
	}
}

// IsValidForCounter if an AggregationType is valid for Counter.
func (a AggregationType) IsValidForCounter() bool {
	switch a {
	case Min, Max, Mean, Count, Sum, SumSq, Stdev:
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

// Quantile returns the quantile represented by the AggregationType.
func (a AggregationType) Quantile() (float64, bool) {
	switch a {
	case P10:
		return 0.1, true
	case P20:
		return 0.2, true
	case P30:
		return 0.3, true
	case P40:
		return 0.4, true
	case P50, Median:
		return 0.5, true
	case P60:
		return 0.6, true
	case P70:
		return 0.7, true
	case P80:
		return 0.8, true
	case P90:
		return 0.9, true
	case P95:
		return 0.95, true
	case P99:
		return 0.99, true
	case P999:
		return 0.999, true
	case P9999:
		return 0.9999, true
	default:
		return 0, false
	}
}

// Schema returns the schema of the aggregation type.
func (a AggregationType) Schema() (schema.AggregationType, error) {
	s := schema.AggregationType(a)
	if err := validateSchemaAggregationType(s); err != nil {
		return schema.AggregationType_UNKNOWN, err
	}
	return s, nil
}

// UnmarshalYAML unmarshals aggregation type from a string.
func (a *AggregationType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := ParseAggregationType(str)
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}

func validateSchemaAggregationType(a schema.AggregationType) error {
	_, ok := schema.AggregationType_name[int32(a)]
	if !ok {
		return fmt.Errorf("invalid schema aggregation type: %v", a)
	}
	return nil
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

// UnmarshalYAML unmarshals aggregation types from a string.
func (aggTypes *AggregationTypes) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := ParseAggregationTypes(str)
	if err != nil {
		return err
	}
	*aggTypes = parsed
	return nil
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

// PooledQuantiles returns all the quantiles found in the list
// of aggregation types. Using a floats pool if available.
//
// A boolean will also be returned to indicate whether the
// returned float slice is from the pool.
func (aggTypes AggregationTypes) PooledQuantiles(p pool.FloatsPool) ([]float64, bool) {
	var (
		res         []float64
		initialized bool
		medianAdded bool
		pooled      bool
	)
	for _, aggType := range aggTypes {
		q, ok := aggType.Quantile()
		if !ok {
			continue
		}
		// Dedup P50 and Median.
		if aggType == P50 || aggType == Median {
			if medianAdded {
				continue
			}
			medianAdded = true
		}
		if !initialized {
			if p == nil {
				res = make([]float64, 0, len(aggTypes))
			} else {
				res = p.Get(len(aggTypes))
				pooled = true
			}
			initialized = true
		}
		res = append(res, q)
	}
	return res, pooled
}

// Schema returns the schema of the aggregation types.
func (aggTypes AggregationTypes) Schema() ([]schema.AggregationType, error) {
	// This is the same as returning an empty slice from the functionality perspective.
	// It makes creating testing fixtures much simpler.
	if aggTypes == nil {
		return nil, nil
	}

	res := make([]schema.AggregationType, len(aggTypes))
	for i, aggType := range aggTypes {
		s, err := aggType.Schema()
		if err != nil {
			return nil, err
		}
		res[i] = s
	}

	return res, nil
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
