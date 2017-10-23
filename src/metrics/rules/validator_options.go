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

package rules

import (
	"fmt"
	"strconv"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

// MetricTypesFn determines the possible metric types based on a set of tag based filters.
type MetricTypesFn func(tagFilters map[string]string) ([]metric.Type, error)

// ValidatorOptions provide a set of options for the validator.
type ValidatorOptions interface {
	// SetDefaultAllowedStoragePolicies sets the default list of allowed storage policies.
	SetDefaultAllowedStoragePolicies(value []policy.StoragePolicy) ValidatorOptions

	// SetDefaultAllowedCustomAggregationTypes sets the default list of allowed custom
	// aggregation types.
	SetDefaultAllowedCustomAggregationTypes(value policy.AggregationTypes) ValidatorOptions

	// SetAllowedStoragePoliciesFor sets the list of allowed storage policies for a given metric type.
	SetAllowedStoragePoliciesFor(t metric.Type, policies []policy.StoragePolicy) ValidatorOptions

	// SetAllowedCustomAggregationTypesFor sets the list of allowed custom aggregation
	// types for a given metric type.
	SetAllowedCustomAggregationTypesFor(t metric.Type, aggTypes policy.AggregationTypes) ValidatorOptions

	// SetMetricTypesFn sets the metric types function.
	SetMetricTypesFn(value MetricTypesFn) ValidatorOptions

	// MetricTypesFn returns the metric types function.
	MetricTypesFn() MetricTypesFn

	// SetRequiredRollupTags sets the list of required rollup tags.
	SetRequiredRollupTags(value []string) ValidatorOptions

	// RequiredRollupTags returns the list of required rollup tags.
	RequiredRollupTags() []string

	// SetTagNameInvalidChars sets the list of invalid chars for a tag name.
	SetTagNameInvalidChars(value []rune) ValidatorOptions

	// CheckInvalidCharactersForTagName checks if the given tag name contains invalid characters
	// returning an error if invalid character(s) present.
	CheckInvalidCharactersForTagName(tagName string) error

	// SetMetricNameInvalidChars sets the list of invalid chars for a metric name.
	SetMetricNameInvalidChars(value []rune) ValidatorOptions

	// CheckInvalidCharactersForMetricName checks if the given metric name contains invalid characters
	// returning an error if invalid character(s) present.
	CheckInvalidCharactersForMetricName(metricName string) error

	// IsAllowedStoragePolicyFor determines whether a given storage policy is allowed for the
	// given metric type.
	IsAllowedStoragePolicyFor(t metric.Type, p policy.StoragePolicy) bool

	// IsAllowedCustomAggregationTypeFor determines whether a given aggregation type is allowed for
	// the given metric type.
	IsAllowedCustomAggregationTypeFor(t metric.Type, aggType policy.AggregationType) bool
}

type validationMetadata struct {
	allowedStoragePolicies map[policy.StoragePolicy]struct{}
	allowedCustomAggTypes  map[policy.AggregationType]struct{}
}

type validatorOptions struct {
	defaultAllowedStoragePolicies        map[policy.StoragePolicy]struct{}
	defaultAllowedCustomAggregationTypes map[policy.AggregationType]struct{}
	metricTypesFn                        MetricTypesFn
	requiredRollupTags                   []string
	metricNameInvalidChars               map[rune]struct{}
	tagNameInvalidChars                  map[rune]struct{}
	metadatasByType                      map[metric.Type]validationMetadata
}

// NewValidatorOptions create a new set of validator options.
func NewValidatorOptions() ValidatorOptions {
	return &validatorOptions{
		metadatasByType: make(map[metric.Type]validationMetadata),
	}
}

func (o *validatorOptions) SetDefaultAllowedStoragePolicies(value []policy.StoragePolicy) ValidatorOptions {
	o.defaultAllowedStoragePolicies = toStoragePolicySet(value)
	return o
}

func (o *validatorOptions) SetDefaultAllowedCustomAggregationTypes(value policy.AggregationTypes) ValidatorOptions {
	o.defaultAllowedCustomAggregationTypes = toAggregationTypeSet(value)
	return o
}

func (o *validatorOptions) SetAllowedStoragePoliciesFor(t metric.Type, policies []policy.StoragePolicy) ValidatorOptions {
	metadata := o.findOrCreateMetadata(t)
	metadata.allowedStoragePolicies = toStoragePolicySet(policies)
	o.metadatasByType[t] = metadata
	return o
}

func (o *validatorOptions) SetAllowedCustomAggregationTypesFor(t metric.Type, aggTypes policy.AggregationTypes) ValidatorOptions {
	metadata := o.findOrCreateMetadata(t)
	metadata.allowedCustomAggTypes = toAggregationTypeSet(aggTypes)
	o.metadatasByType[t] = metadata
	return o
}

func (o *validatorOptions) SetMetricTypesFn(value MetricTypesFn) ValidatorOptions {
	o.metricTypesFn = value
	return o
}

func (o *validatorOptions) MetricTypesFn() MetricTypesFn {
	return o.metricTypesFn
}

func (o *validatorOptions) SetRequiredRollupTags(value []string) ValidatorOptions {
	requiredRollupTags := make([]string, len(value))
	copy(requiredRollupTags, value)
	opts := *o
	opts.requiredRollupTags = requiredRollupTags
	return &opts
}

func (o *validatorOptions) RequiredRollupTags() []string {
	return o.requiredRollupTags
}

func (o *validatorOptions) SetTagNameInvalidChars(values []rune) ValidatorOptions {
	tagNameInvalidChars := make(map[rune]struct{}, len(values))
	for _, v := range values {
		tagNameInvalidChars[v] = struct{}{}
	}
	opts := *o
	opts.tagNameInvalidChars = tagNameInvalidChars
	return &opts
}

func (o *validatorOptions) CheckInvalidCharactersForTagName(tagName string) error {
	return validateChars(tagName, o.tagNameInvalidChars)
}

func (o *validatorOptions) SetMetricNameInvalidChars(values []rune) ValidatorOptions {
	metricNameInvalidChars := make(map[rune]struct{}, len(values))
	for _, v := range values {
		metricNameInvalidChars[v] = struct{}{}
	}
	opts := *o
	opts.metricNameInvalidChars = metricNameInvalidChars
	return &opts
}

func (o *validatorOptions) CheckInvalidCharactersForMetricName(metricName string) error {
	return validateChars(metricName, o.metricNameInvalidChars)
}

func (o *validatorOptions) IsAllowedStoragePolicyFor(t metric.Type, p policy.StoragePolicy) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		_, found := metadata.allowedStoragePolicies[p]
		return found
	}
	_, found := o.defaultAllowedStoragePolicies[p]
	return found
}

func (o *validatorOptions) IsAllowedCustomAggregationTypeFor(t metric.Type, aggType policy.AggregationType) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		_, found := metadata.allowedCustomAggTypes[aggType]
		return found
	}
	_, found := o.defaultAllowedCustomAggregationTypes[aggType]
	return found
}

func (o *validatorOptions) findOrCreateMetadata(t metric.Type) validationMetadata {
	if metadata, found := o.metadatasByType[t]; found {
		return metadata
	}
	return validationMetadata{
		allowedStoragePolicies: o.defaultAllowedStoragePolicies,
		allowedCustomAggTypes:  o.defaultAllowedCustomAggregationTypes,
	}
}

func toStoragePolicySet(policies []policy.StoragePolicy) map[policy.StoragePolicy]struct{} {
	m := make(map[policy.StoragePolicy]struct{}, len(policies))
	for _, p := range policies {
		m[p] = struct{}{}
	}
	return m
}

func toAggregationTypeSet(aggTypes policy.AggregationTypes) map[policy.AggregationType]struct{} {
	m := make(map[policy.AggregationType]struct{}, len(aggTypes))
	for _, t := range aggTypes {
		m[t] = struct{}{}
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
