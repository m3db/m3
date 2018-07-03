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

package validator

import (
	"fmt"
	"strconv"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/validator/namespace"
	"github.com/m3db/m3metrics/rules/validator/namespace/static"
)

const (
	// By default we only support at most one binary transformation function in between
	// consecutive rollup operations in a pipeline.
	defaultMaxTransformationDerivativeOrder = 1

	// By default we allow at most one level of rollup in a pipeline.
	defaultMaxRollupLevels = 1
)

// MetricTypesFn determines the possible metric types based on a set of tag based filters.
type MetricTypesFn func(tagFilters filters.TagFilterValueMap) ([]metric.Type, error)

// Options provide a set of options for the validator.
type Options interface {
	// SetNamespaceValidator sets the namespace validator.
	SetNamespaceValidator(value namespace.Validator) Options

	// NamespaceValidator returns the namespace validator.
	NamespaceValidator() namespace.Validator

	// SetDefaultAllowedStoragePolicies sets the default list of allowed storage policies.
	SetDefaultAllowedStoragePolicies(value []policy.StoragePolicy) Options

	// SetDefaultAllowedFirstLevelAggregationTypes sets the default list of allowed first-level
	// aggregation types.
	SetDefaultAllowedFirstLevelAggregationTypes(value aggregation.Types) Options

	// SetDefaultAllowedNonFirstLevelAggregationTypes sets the default list of allowed
	// non-first-level aggregation types.
	SetDefaultAllowedNonFirstLevelAggregationTypes(value aggregation.Types) Options

	// SetAllowedStoragePoliciesFor sets the list of allowed storage policies for a given metric type.
	SetAllowedStoragePoliciesFor(t metric.Type, policies []policy.StoragePolicy) Options

	// SetAllowedFirstLevelAggregationTypesFor sets the list of allowed first-level aggregation
	// types for a given metric type.
	SetAllowedFirstLevelAggregationTypesFor(t metric.Type, aggTypes aggregation.Types) Options

	// SetAllowedNonFirstLevelAggregationTypesFor sets the list of allowed non-first-level
	// aggregation types for a given metric type.
	SetAllowedNonFirstLevelAggregationTypesFor(t metric.Type, aggTypes aggregation.Types) Options

	// SetMetricTypesFn sets the metric types function.
	SetMetricTypesFn(value MetricTypesFn) Options

	// MetricTypesFn returns the metric types function.
	MetricTypesFn() MetricTypesFn

	// SetMultiAggregationTypesEnabledFor sets the list of metric types that support
	// multiple aggregation types.
	SetMultiAggregationTypesEnabledFor(value []metric.Type) Options

	// SetRequiredRollupTags sets the list of required rollup tags.
	SetRequiredRollupTags(value []string) Options

	// RequiredRollupTags returns the list of required rollup tags.
	RequiredRollupTags() []string

	// SetMaxTransformationDerivativeOrder sets the maximum supported transformation
	// derivative order between rollup operations in pipelines.
	SetMaxTransformationDerivativeOrder(value int) Options

	// MaxTransformationDerivativeOrder returns the maximum supported transformation
	// derivative order between rollup operations in pipelines..
	MaxTransformationDerivativeOrder() int

	// SetMaxRollupLevels sets the maximum number of rollup operations supported in pipelines.
	SetMaxRollupLevels(value int) Options

	// MaxRollupLevels returns the maximum number of rollup operations supported in pipelines.
	MaxRollupLevels() int

	// SetTagNameInvalidChars sets the list of invalid chars for a tag name.
	SetTagNameInvalidChars(value []rune) Options

	// CheckInvalidCharactersForTagName checks if the given tag name contains invalid characters
	// returning an error if invalid character(s) present.
	CheckInvalidCharactersForTagName(tagName string) error

	// SetMetricNameInvalidChars sets the list of invalid chars for a metric name.
	SetMetricNameInvalidChars(value []rune) Options

	// CheckInvalidCharactersForMetricName checks if the given metric name contains invalid characters
	// returning an error if invalid character(s) present.
	CheckInvalidCharactersForMetricName(metricName string) error

	// IsAllowedStoragePolicyFor determines whether a given storage policy is allowed for the
	// given metric type.
	IsAllowedStoragePolicyFor(t metric.Type, p policy.StoragePolicy) bool

	// IsMultiAggregationTypesEnabledFor checks if a metric type supports multiple aggregation types.
	IsMultiAggregationTypesEnabledFor(t metric.Type) bool

	// IsAllowedFirstLevelAggregationTypeFor determines whether a given aggregation type is allowed
	// as the first-level aggregation for the given metric type.
	IsAllowedFirstLevelAggregationTypeFor(t metric.Type, aggType aggregation.Type) bool

	// IsAllowedNonFirstLevelAggregationTypeFor determines whether a given aggregation type is
	// allowed as the non-first-level aggregation for the given metric type.
	IsAllowedNonFirstLevelAggregationTypeFor(t metric.Type, aggType aggregation.Type) bool
}

type validationMetadata struct {
	allowedStoragePolicies       map[policy.StoragePolicy]struct{}
	allowedFirstLevelAggTypes    map[aggregation.Type]struct{}
	allowedNonFirstLevelAggTypes map[aggregation.Type]struct{}
}

type options struct {
	namespaceValidator                          namespace.Validator
	defaultAllowedStoragePolicies               map[policy.StoragePolicy]struct{}
	defaultAllowedFirstLevelAggregationTypes    map[aggregation.Type]struct{}
	defaultAllowedNonFirstLevelAggregationTypes map[aggregation.Type]struct{}
	metricTypesFn                               MetricTypesFn
	multiAggregationTypesEnableFor              map[metric.Type]struct{}
	requiredRollupTags                          []string
	maxTransformationDerivativeOrder            int
	maxRollupLevels                             int
	metricNameInvalidChars                      map[rune]struct{}
	tagNameInvalidChars                         map[rune]struct{}
	metadatasByType                             map[metric.Type]validationMetadata
}

// NewOptions create a new set of validator options.
func NewOptions() Options {
	return &options{
		multiAggregationTypesEnableFor:   map[metric.Type]struct{}{metric.TimerType: struct{}{}},
		maxTransformationDerivativeOrder: defaultMaxTransformationDerivativeOrder,
		maxRollupLevels:                  defaultMaxRollupLevels,
		namespaceValidator:               static.NewNamespaceValidator(static.Valid),
		metadatasByType:                  make(map[metric.Type]validationMetadata),
	}
}

func (o *options) SetNamespaceValidator(value namespace.Validator) Options {
	o.namespaceValidator = value
	return o
}

func (o *options) NamespaceValidator() namespace.Validator {
	return o.namespaceValidator
}

func (o *options) SetDefaultAllowedStoragePolicies(value []policy.StoragePolicy) Options {
	o.defaultAllowedStoragePolicies = toStoragePolicySet(value)
	return o
}

func (o *options) SetDefaultAllowedFirstLevelAggregationTypes(value aggregation.Types) Options {
	o.defaultAllowedFirstLevelAggregationTypes = toAggregationTypeSet(value)
	return o
}

func (o *options) SetDefaultAllowedNonFirstLevelAggregationTypes(value aggregation.Types) Options {
	o.defaultAllowedNonFirstLevelAggregationTypes = toAggregationTypeSet(value)
	return o
}

func (o *options) SetAllowedStoragePoliciesFor(t metric.Type, policies []policy.StoragePolicy) Options {
	metadata := o.findOrCreateMetadata(t)
	metadata.allowedStoragePolicies = toStoragePolicySet(policies)
	o.metadatasByType[t] = metadata
	return o
}

func (o *options) SetAllowedFirstLevelAggregationTypesFor(t metric.Type, aggTypes aggregation.Types) Options {
	metadata := o.findOrCreateMetadata(t)
	metadata.allowedFirstLevelAggTypes = toAggregationTypeSet(aggTypes)
	o.metadatasByType[t] = metadata
	return o
}

func (o *options) SetAllowedNonFirstLevelAggregationTypesFor(t metric.Type, aggTypes aggregation.Types) Options {
	metadata := o.findOrCreateMetadata(t)
	metadata.allowedNonFirstLevelAggTypes = toAggregationTypeSet(aggTypes)
	o.metadatasByType[t] = metadata
	return o
}

func (o *options) SetMetricTypesFn(value MetricTypesFn) Options {
	o.metricTypesFn = value
	return o
}

func (o *options) MetricTypesFn() MetricTypesFn {
	return o.metricTypesFn
}

func (o *options) SetMultiAggregationTypesEnabledFor(value []metric.Type) Options {
	o.multiAggregationTypesEnableFor = toMetricTypeSet(value)
	return o
}

func (o *options) SetRequiredRollupTags(value []string) Options {
	requiredRollupTags := make([]string, len(value))
	copy(requiredRollupTags, value)
	o.requiredRollupTags = requiredRollupTags
	return o
}

func (o *options) RequiredRollupTags() []string {
	return o.requiredRollupTags
}

func (o *options) SetMaxTransformationDerivativeOrder(value int) Options {
	o.maxTransformationDerivativeOrder = value
	return o
}

func (o *options) MaxTransformationDerivativeOrder() int {
	return o.maxTransformationDerivativeOrder
}

func (o *options) SetMaxRollupLevels(value int) Options {
	o.maxRollupLevels = value
	return o
}

func (o *options) MaxRollupLevels() int {
	return o.maxRollupLevels
}

func (o *options) SetTagNameInvalidChars(values []rune) Options {
	tagNameInvalidChars := make(map[rune]struct{}, len(values))
	for _, v := range values {
		tagNameInvalidChars[v] = struct{}{}
	}
	o.tagNameInvalidChars = tagNameInvalidChars
	return o
}

func (o *options) CheckInvalidCharactersForTagName(tagName string) error {
	return validateChars(tagName, o.tagNameInvalidChars)
}

func (o *options) SetMetricNameInvalidChars(values []rune) Options {
	metricNameInvalidChars := make(map[rune]struct{}, len(values))
	for _, v := range values {
		metricNameInvalidChars[v] = struct{}{}
	}
	o.metricNameInvalidChars = metricNameInvalidChars
	return o
}

func (o *options) CheckInvalidCharactersForMetricName(metricName string) error {
	return validateChars(metricName, o.metricNameInvalidChars)
}

func (o *options) IsAllowedStoragePolicyFor(t metric.Type, p policy.StoragePolicy) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		_, found := metadata.allowedStoragePolicies[p]
		return found
	}
	_, found := o.defaultAllowedStoragePolicies[p]
	return found
}

func (o *options) IsMultiAggregationTypesEnabledFor(t metric.Type) bool {
	_, exists := o.multiAggregationTypesEnableFor[t]
	return exists
}

func (o *options) IsAllowedFirstLevelAggregationTypeFor(t metric.Type, aggType aggregation.Type) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		_, found := metadata.allowedFirstLevelAggTypes[aggType]
		return found
	}
	_, found := o.defaultAllowedFirstLevelAggregationTypes[aggType]
	return found
}

func (o *options) IsAllowedNonFirstLevelAggregationTypeFor(t metric.Type, aggType aggregation.Type) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		_, found := metadata.allowedNonFirstLevelAggTypes[aggType]
		return found
	}
	_, found := o.defaultAllowedNonFirstLevelAggregationTypes[aggType]
	return found
}

func (o *options) findOrCreateMetadata(t metric.Type) validationMetadata {
	if metadata, found := o.metadatasByType[t]; found {
		return metadata
	}
	return validationMetadata{
		allowedStoragePolicies:       o.defaultAllowedStoragePolicies,
		allowedFirstLevelAggTypes:    o.defaultAllowedFirstLevelAggregationTypes,
		allowedNonFirstLevelAggTypes: o.defaultAllowedNonFirstLevelAggregationTypes,
	}
}

func toStoragePolicySet(policies []policy.StoragePolicy) map[policy.StoragePolicy]struct{} {
	m := make(map[policy.StoragePolicy]struct{}, len(policies))
	for _, p := range policies {
		m[p] = struct{}{}
	}
	return m
}

func toAggregationTypeSet(aggTypes aggregation.Types) map[aggregation.Type]struct{} {
	m := make(map[aggregation.Type]struct{}, len(aggTypes))
	for _, t := range aggTypes {
		m[t] = struct{}{}
	}
	return m
}

func toMetricTypeSet(metricTypes []metric.Type) map[metric.Type]struct{} {
	m := make(map[metric.Type]struct{}, len(metricTypes))
	for _, mt := range metricTypes {
		m[mt] = struct{}{}
	}
	return m
}

func validateChars(str string, invalidChars map[rune]struct{}) error {
	if len(invalidChars) == 0 {
		return nil
	}

	// Validate that given string doesn't contain an invalid character.
	for _, char := range str {
		if _, exists := invalidChars[char]; exists {
			return fmt.Errorf("%s contains invalid character %s", str, strconv.QuoteRune(char))
		}
	}
	return nil
}
