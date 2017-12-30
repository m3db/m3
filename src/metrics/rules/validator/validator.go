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

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/validator/namespace"
)

type validator struct {
	opts        Options
	nsValidator namespace.Validator
}

// NewValidator creates a new validator.
func NewValidator(opts Options) rules.Validator {
	return &validator{
		opts:        opts,
		nsValidator: opts.NamespaceValidator(),
	}
}

func (v *validator) Validate(rs rules.RuleSet) error {
	// Only the latest (a.k.a. the first) view needs to be validated
	// because that is the view that may be invalid due to latest update.
	latest, err := rs.Latest()
	if err != nil {
		return v.wrapError(fmt.Errorf("could not get the latest ruleset snapshot: %v", err))
	}
	return v.ValidateSnapshot(latest)
}

func (v *validator) ValidateSnapshot(snapshot *rules.RuleSetSnapshot) error {
	if snapshot == nil {
		return nil
	}
	if err := v.validateSnapshot(snapshot); err != nil {
		return v.wrapError(err)
	}
	return nil
}

func (v *validator) Close() {
	v.nsValidator.Close()
}

func (v *validator) validateSnapshot(snapshot *rules.RuleSetSnapshot) error {
	if err := v.validateNamespace(snapshot.Namespace); err != nil {
		return err
	}
	if err := v.validateMappingRules(snapshot.MappingRules); err != nil {
		return err
	}
	return v.validateRollupRules(snapshot.RollupRules)
}

func (v *validator) validateNamespace(ns string) error {
	return v.nsValidator.Validate(ns)
}

func (v *validator) validateMappingRules(mrv map[string]*rules.MappingRuleView) error {
	namesSeen := make(map[string]struct{}, len(mrv))
	for _, view := range mrv {
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			return NewRuleConflictError(fmt.Sprintf("mapping rule %s already exists", view.Name))
		}
		namesSeen[view.Name] = struct{}{}

		// Validate that the filter is valid.
		filterValues, err := v.validateFilter(view.Name, view.Filter)
		if err != nil {
			return err
		}

		// Validate the metric types.
		types, err := v.opts.MetricTypesFn()(filterValues)
		if err != nil {
			return err
		}
		if len(types) == 0 {
			return fmt.Errorf("mapping rule %s does not match any allowed metric types, filter=%s", view.Name, view.Filter)
		}

		// Validate that the policies are valid.
		if err := v.validatePolicies(view.Name, view.Policies, types); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateRollupRules(rrv map[string]*rules.RollupRuleView) error {
	var (
		namesSeen   = make(map[string]struct{}, len(rrv))
		targetsSeen = make([]rules.RollupTarget, 0, len(rrv))
	)
	for _, view := range rrv {
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			return NewRuleConflictError(fmt.Sprintf("rollup rule %s already exists", view.Name))
		}
		namesSeen[view.Name] = struct{}{}

		// Validate that the filter is valid.
		filterValues, err := v.validateFilter(view.Name, view.Filter)
		if err != nil {
			return err
		}

		// Validate the metric types.
		types, err := v.opts.MetricTypesFn()(filterValues)
		if err != nil {
			return err
		}
		if len(types) == 0 {
			return fmt.Errorf("rollup rule %s does not match any allowed metric types, filter=%s", view.Name, view.Filter)
		}

		for _, target := range view.Targets {
			// Validate that rollup metric name is valid.
			if err := v.validateRollupMetricName(view.Name, target.Name); err != nil {
				return err
			}

			// Validate that the rollup tags are valid.
			if err := v.validateRollupTags(view.Name, target.Tags); err != nil {
				return err
			}

			// Validate that the policies are valid.
			if err := v.validatePolicies(view.Name, target.Policies, types); err != nil {
				return err
			}

			// Validate that there are no conflicting rollup targets.
			current := target.RollupTarget()
			for _, seenTarget := range targetsSeen {
				if current.SameTransform(seenTarget) {
					return NewRuleConflictError(fmt.Sprintf("rollup target with name %s and tags %s already exists", current.Name, current.Tags))
				}
			}
			targetsSeen = append(targetsSeen, current)
		}
	}

	return nil
}

func (v *validator) validateFilter(ruleName string, f string) (filters.TagFilterValueMap, error) {
	filterValues, err := filters.ParseTagFilterValueMap(f)
	if err != nil {
		return nil, fmt.Errorf("rule %s has invalid rule filter %s: %v", ruleName, f, err)
	}
	for tag, filterValue := range filterValues {
		// Validating the filter tag name does not contain invalid chars.
		if err := v.opts.CheckInvalidCharactersForTagName(tag); err != nil {
			return nil, fmt.Errorf("rule %s has invalid rule filter %s: tag name %s contains invalid character, err: %v", ruleName, f, tag, err)
		}

		// Validating the filter expression by actually constructing the filter.
		if _, err := filters.NewFilterFromFilterValue(filterValue); err != nil {
			return nil, fmt.Errorf("rule %s has invalid rule filter %s: filter pattern for tag %s is invalid, err: %v", ruleName, f, tag, err)
		}
	}
	return filterValues, nil
}

func (v *validator) validatePolicies(ruleName string, policies []policy.Policy, types []metric.Type) error {
	// Validating that at least one policy is provided.
	if len(policies) == 0 {
		return fmt.Errorf("rule %s has no policies", ruleName)
	}

	// Validating that no duplicate policies exist.
	seen := make(map[policy.Policy]struct{}, len(policies))
	for _, p := range policies {
		if _, exists := seen[p]; exists {
			return fmt.Errorf("rule %s has duplicate policy %s, provided policies are %v", ruleName, p.String(), policies)
		}
		seen[p] = struct{}{}
	}

	// Validating that provided policies are allowed for the specified metric type.
	for _, t := range types {
		for _, p := range policies {
			if err := v.validatePolicy(t, p); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *validator) validateRollupTags(ruleName string, tags []string) error {
	// Validating that all tag names have valid characters.
	for _, tag := range tags {
		if err := v.opts.CheckInvalidCharactersForTagName(tag); err != nil {
			return fmt.Errorf("rollup rule %s has invalid rollup tag %s: %v", ruleName, tag, err)
		}
	}

	// Validating that there are no duplicate rollup tags.
	rollupTags := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if _, exists := rollupTags[tag]; exists {
			return fmt.Errorf("rollup rule %s has duplicate rollup tag: %s, provided rollup tags are %v", ruleName, tag, tags)
		}
		rollupTags[tag] = struct{}{}
	}

	// Validating the list of rollup tags in the rule contain all required tags.
	requiredTags := v.opts.RequiredRollupTags()
	if len(requiredTags) == 0 {
		return nil
	}
	for _, requiredTag := range requiredTags {
		if _, exists := rollupTags[requiredTag]; !exists {
			return fmt.Errorf("rollup rule %s does not have required rollup tag: %s, provided rollup tags are %v", ruleName, requiredTag, tags)
		}
	}

	return nil
}

func (v *validator) validateRollupMetricName(ruleName, metricName string) error {
	// Validate that rollup metric name is not empty.
	if metricName == "" {
		return fmt.Errorf("rollup rule %s has an empty rollup metric name", ruleName)
	}

	// Validate that rollup metric name has valid characters.
	if err := v.opts.CheckInvalidCharactersForMetricName(metricName); err != nil {
		return fmt.Errorf("rollup rule %s has an invalid rollup metric name %s: %v", ruleName, metricName, err)
	}

	return nil
}

func (v *validator) validatePolicy(t metric.Type, p policy.Policy) error {
	// Validate storage policy.
	if !v.opts.IsAllowedStoragePolicyFor(t, p.StoragePolicy) {
		return fmt.Errorf("storage policy %v is not allowed for metric type %v", p.StoragePolicy, t)
	}

	// Validate aggregation function.
	if isDefaultAggFn := p.AggregationID.IsDefault(); isDefaultAggFn {
		return nil
	}
	aggTypes, err := p.AggregationID.AggregationTypes()
	if err != nil {
		return err
	}
	for _, aggType := range aggTypes {
		if !v.opts.IsAllowedCustomAggregationTypeFor(t, aggType) {
			return fmt.Errorf("aggregation type %v is not allowed for metric type %v", aggType, t)
		}
	}

	return nil
}

func (v *validator) wrapError(err error) error {
	if err == nil {
		return nil
	}
	switch err.(type) {
	// Do not wrap error for these error types so caller can take actions based on the correct
	// error type.
	case RuleConflictError:
		return err
	default:
		return NewValidationError(err.Error())
	}
}
