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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/aggregation"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric"
	mpipeline "github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/validator/namespace"
	"github.com/m3db/m3/src/metrics/rules/view"
)

var (
	errNoStoragePolicies                  = errors.New("no storage policies")
	errEmptyRollupMetricName              = errors.New("empty rollup metric name")
	errEmptyPipeline                      = errors.New("empty pipeline")
	errMoreThanOneAggregationOpInPipeline = errors.New("more than one aggregation operation in pipeline")
	errAggregationOpNotFirstInPipeline    = errors.New("aggregation operation is not the first operation in pipeline")
	errNoRollupOpInPipeline               = errors.New("no rollup operation in pipeline")
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

func (v *validator) ValidateSnapshot(snapshot view.RuleSet) error {
	if err := v.validateSnapshot(snapshot); err != nil {
		return v.wrapError(err)
	}
	return nil
}

func (v *validator) Close() {
	v.nsValidator.Close()
}

func (v *validator) validateSnapshot(snapshot view.RuleSet) error {
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

func (v *validator) validateMappingRules(mrv []view.MappingRule) error {
	namesSeen := make(map[string]struct{}, len(mrv))
	for _, rule := range mrv {
		if rule.Tombstoned {
			continue
		}
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[rule.Name]; exists {
			return merrors.NewInvalidInputError(fmt.Sprintf("mapping rule '%s' already exists", rule.Name))
		}
		namesSeen[rule.Name] = struct{}{}

		// Validate that the filter is valid.
		filterValues, err := v.validateFilter(rule.Filter)
		if err != nil {
			return fmt.Errorf("mapping rule '%s' has invalid filter %s: %v", rule.Name, rule.Filter, err)
		}

		// Validate the metric types.
		types, err := v.opts.MetricTypesFn()(filterValues)
		if err != nil {
			return fmt.Errorf("mapping rule '%s' cannot infer metric types from filter %v: %v", rule.Name, rule.Filter, err)
		}
		if len(types) == 0 {
			return fmt.Errorf("mapping rule '%s' does not match any allowed metric types, filter=%s", rule.Name, rule.Filter)
		}

		// Validate the aggregation ID.
		if err := v.validateAggregationID(rule.AggregationID, firstLevelAggregationType, types); err != nil {
			return fmt.Errorf("mapping rule '%s' has invalid aggregation ID %v: %v", rule.Name, rule.AggregationID, err)
		}

		// Validate the drop policy is valid.
		if !rule.DropPolicy.IsValid() {
			return fmt.Errorf("mapping rule '%s' has an invalid drop policy: value=%d, string=%s, valid_values=%v",
				rule.Name, int(rule.DropPolicy), rule.DropPolicy.String(), policy.ValidDropPolicies())
		}

		// Validate the storage policies if drop policy not active, otherwise ensure none.
		if rule.DropPolicy.IsDefault() {
			// Drop policy not set, validate that the storage policies are valid.
			if err := v.validateStoragePolicies(rule.StoragePolicies, types); err != nil {
				return fmt.Errorf("mapping rule '%s' has invalid storage policies in %v: %v", rule.Name, rule.StoragePolicies, err)
			}
		} else {
			// Drop policy is set, ensure default aggregation ID and no storage policies set.
			if !rule.AggregationID.IsDefault() {
				return fmt.Errorf("mapping rule '%s' has a drop policy error: must use default aggregation ID", rule.Name)
			}
			if len(rule.StoragePolicies) != 0 {
				return fmt.Errorf("mapping rule '%s' has a drop policy error: cannot specify storage policies", rule.Name)
			}
		}
	}
	return nil
}

func (v *validator) validateRollupRules(rrv []view.RollupRule) error {
	var (
		namesSeen = make(map[string]struct{}, len(rrv))
		pipelines = make([]mpipeline.Pipeline, 0, len(rrv))
	)
	for _, rule := range rrv {
		if rule.Tombstoned {
			continue
		}
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[rule.Name]; exists {
			return merrors.NewInvalidInputError(fmt.Sprintf("rollup rule '%s' already exists", rule.Name))
		}
		namesSeen[rule.Name] = struct{}{}

		// Validate that the filter is valid.
		filterValues, err := v.validateFilter(rule.Filter)
		if err != nil {
			return fmt.Errorf("rollup rule '%s' has invalid filter %s: %v", rule.Name, rule.Filter, err)
		}

		// Validate the metric types.
		types, err := v.opts.MetricTypesFn()(filterValues)
		if err != nil {
			return fmt.Errorf("rollup rule '%s' cannot infer metric types from filter %v: %v", rule.Name, rule.Filter, err)
		}
		if len(types) == 0 {
			return fmt.Errorf("rollup rule '%s' does not match any allowed metric types, filter=%s", rule.Name, rule.Filter)
		}

		for _, target := range rule.Targets {
			// Validate the pipeline is valid.
			if err := v.validatePipeline(target.Pipeline, types); err != nil {
				return fmt.Errorf("rollup rule '%s' has invalid pipeline '%v': %v", rule.Name, target.Pipeline, err)
			}

			// Validate that the storage policies are valid.
			if err := v.validateStoragePolicies(target.StoragePolicies, types); err != nil {
				return fmt.Errorf("rollup rule '%s' has invalid storage policies in %v: %v", rule.Name, target.StoragePolicies, err)
			}
			pipelines = append(pipelines, target.Pipeline)
		}
	}

	return validateNoDuplicateRollupIDIn(pipelines)
}

func (v *validator) validateFilter(f string) (filters.TagFilterValueMap, error) {
	filterValues, err := filters.ValidateTagsFilter(f)
	if err != nil {
		return nil, err
	}
	for tag := range filterValues {
		// Validating the filter tag name does not contain invalid chars.
		if err := v.opts.CheckInvalidCharactersForTagName(tag); err != nil {
			return nil, fmt.Errorf("tag name '%s' contains invalid character, err: %v", tag, err)
		}
		if err := v.opts.CheckFilterTagNameValid(tag); err != nil {
			return nil, err
		}
	}
	return filterValues, nil
}

func (v *validator) validateAggregationID(
	aggregationID aggregation.ID,
	aggregationType aggregationType,
	types []metric.Type,
) error {
	// Default aggregation types are always allowed.
	if aggregationID.IsDefault() {
		return nil
	}
	aggTypes, err := aggregationID.Types()
	if err != nil {
		return err
	}
	if len(aggTypes) > 1 {
		for _, t := range types {
			if !v.opts.IsMultiAggregationTypesEnabledFor(t) {
				return fmt.Errorf("metric type %v does not support multiple aggregation types %v", t, aggTypes)
			}
		}
	}
	isAllowedAggregationTypeForFn := v.opts.IsAllowedFirstLevelAggregationTypeFor
	if aggregationType == nonFirstLevelAggregationType {
		isAllowedAggregationTypeForFn = v.opts.IsAllowedNonFirstLevelAggregationTypeFor
	}
	for _, t := range types {
		for _, aggType := range aggTypes {
			if !isAllowedAggregationTypeForFn(t, aggType) {
				return fmt.Errorf("aggregation type %v is not allowed for metric type %v", aggType, t)
			}
		}
	}
	return nil
}

func (v *validator) validateStoragePolicies(
	storagePolicies policy.StoragePolicies,
	types []metric.Type,
) error {
	// Validating that at least one storage policy is provided.
	if len(storagePolicies) == 0 {
		return errNoStoragePolicies
	}

	// Validating that no duplicate storage policies exist.
	seen := make(map[policy.StoragePolicy]struct{}, len(storagePolicies))
	for _, sp := range storagePolicies {
		if _, exists := seen[sp]; exists {
			return fmt.Errorf("duplicate storage policy '%s'", sp.String())
		}
		seen[sp] = struct{}{}
	}

	// Validating that provided storage policies are allowed for the specified metric type.
	for _, t := range types {
		for _, sp := range storagePolicies {
			if !v.opts.IsAllowedStoragePolicyFor(t, sp) {
				return fmt.Errorf("storage policy '%s' is not allowed for metric type %v", sp.String(), t)
			}
		}
	}
	return nil
}

// validatePipeline validates the rollup pipeline as follows:
// * The pipeline must contain at least one operation.
// * The pipeline can contain at most one aggregation operation, and if there is one,
//   it must be the first operation.
// * The pipeline can contain arbitrary number of transformation operations. However,
//   the transformation derivative order computed from the list of transformations must
//   be no more than the maximum transformation derivative order that is supported.
// * The pipeline must contain at least one rollup operation and at most `n` rollup operations,
//   where `n` is the maximum supported number of rollup levels.
func (v *validator) validatePipeline(pipeline mpipeline.Pipeline, types []metric.Type) error {
	if pipeline.IsEmpty() {
		return errEmptyPipeline
	}
	var (
		numAggregationOps             int
		transformationDerivativeOrder int
		numRollupOps                  int
		previousRollupTags            map[string]struct{}
		numPipelineOps                = pipeline.Len()
	)
	for i := 0; i < numPipelineOps; i++ {
		pipelineOp := pipeline.At(i)
		switch pipelineOp.Type {
		case mpipeline.AggregationOpType:
			numAggregationOps++
			if numAggregationOps > 1 {
				return errMoreThanOneAggregationOpInPipeline
			}
			if i != 0 {
				return errAggregationOpNotFirstInPipeline
			}
			if err := v.validateAggregationOp(pipelineOp.Aggregation, types); err != nil {
				return fmt.Errorf("invalid aggregation operation at index %d: %v", i, err)
			}
		case mpipeline.TransformationOpType:
			transformOp := pipelineOp.Transformation
			if transformOp.Type.IsBinaryTransform() {
				transformationDerivativeOrder++
				if transformationDerivativeOrder > v.opts.MaxTransformationDerivativeOrder() {
					return fmt.Errorf("transformation derivative order is %d higher than supported %d", transformationDerivativeOrder, v.opts.MaxTransformationDerivativeOrder())
				}
			}
			if err := validateTransformationOp(transformOp); err != nil {
				return fmt.Errorf("invalid transformation operation at index %d: %v", i, err)
			}
		case mpipeline.RollupOpType:
			// We only care about the derivative order of transformation operations in between
			// two consecutive rollup operations and as such we reset the derivative order when
			// encountering a rollup operation.
			transformationDerivativeOrder = 0
			numRollupOps++
			if numRollupOps > v.opts.MaxRollupLevels() {
				return fmt.Errorf("number of rollup levels is %d higher than supported %d", numRollupOps, v.opts.MaxRollupLevels())
			}
			if err := v.validateRollupOp(pipelineOp.Rollup, i, types, previousRollupTags); err != nil {
				return fmt.Errorf("invalid rollup operation at index %d: %v", i, err)
			}
			previousRollupTags = make(map[string]struct{}, len(pipelineOp.Rollup.Tags))
			for _, tag := range pipelineOp.Rollup.Tags {
				previousRollupTags[string(tag)] = struct{}{}
			}
		default:
			return fmt.Errorf("operation at index %d has invalid type: %v", i, pipelineOp.Type)
		}
	}
	if numRollupOps == 0 {
		return errNoRollupOpInPipeline
	}
	return nil
}

func (v *validator) validateAggregationOp(
	aggregationOp mpipeline.AggregationOp,
	types []metric.Type,
) error {
	aggregationID, err := aggregation.CompressTypes(aggregationOp.Type)
	if err != nil {
		return err
	}
	return v.validateAggregationID(aggregationID, firstLevelAggregationType, types)
}

func validateTransformationOp(transformationOp mpipeline.TransformationOp) error {
	if !transformationOp.Type.IsValid() {
		return fmt.Errorf("invalid transformation type: %v", transformationOp.Type)
	}
	return nil
}

func (v *validator) validateRollupOp(
	rollupOp mpipeline.RollupOp,
	opIdxInPipeline int,
	types []metric.Type,
	previousRollupTags map[string]struct{},
) error {
	newName := rollupOp.NewName([]byte(""))
	// Validate that the rollup metric name is valid.
	if err := v.validateRollupMetricName(newName); err != nil {
		return fmt.Errorf("invalid rollup metric name '%s': %w", newName, err)
	}

	// Validate that the rollup tags are valid.
	if err := v.validateRollupTags(rollupOp.Tags, previousRollupTags); err != nil {
		return fmt.Errorf("invalid rollup tags %v: %w", rollupOp.Tags, err)
	}

	// Validate that the aggregation ID is valid.
	aggType := firstLevelAggregationType
	if opIdxInPipeline > 0 {
		aggType = nonFirstLevelAggregationType
	}
	if err := v.validateAggregationID(rollupOp.AggregationID, aggType, types); err != nil {
		return fmt.Errorf("invalid aggregation ID %v: %w", rollupOp.AggregationID, err)
	}

	return nil
}

func (v *validator) validateRollupMetricName(metricName []byte) error {
	// Validate that rollup metric name is not empty.
	if len(metricName) == 0 {
		return errEmptyRollupMetricName
	}

	// Validate that rollup metric name has valid characters.
	return v.opts.CheckInvalidCharactersForMetricName(string(metricName))
}

func (v *validator) validateRollupTags(
	tags [][]byte,
	previousRollupTags map[string]struct{},
) error {
	// Validating that all tag names have valid characters.
	for _, tag := range tags {
		if err := v.opts.CheckInvalidCharactersForTagName(string(tag)); err != nil {
			return fmt.Errorf("invalid rollup tag '%s': %v", tag, err)
		}
	}

	// Validating that there are no duplicate rollup tags.
	rollupTags := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagStr := string(tag)
		if _, exists := rollupTags[tagStr]; exists {
			return fmt.Errorf("duplicate rollup tag: '%s'", tagStr)
		}
		rollupTags[tagStr] = struct{}{}
	}

	// Validate that the set of rollup tags are a strict subset of those in
	// previous rollup operations.
	// NB: `previousRollupTags` is nil for the first rollup operation.
	if previousRollupTags != nil {
		var numSeenTags int
		for _, tag := range tags {
			if _, exists := previousRollupTags[string(tag)]; !exists {
				return fmt.Errorf("tag %s not found in previous rollup operations", tag)
			}
			numSeenTags++
		}
		if numSeenTags == len(previousRollupTags) {
			return fmt.Errorf("same set of %d rollup tags in consecutive rollup operations", numSeenTags)
		}
	}

	// Validating the list of rollup tags in the rule contain all required tags.
	requiredTags := v.opts.RequiredRollupTags()
	if len(requiredTags) == 0 {
		return nil
	}
	for _, requiredTag := range requiredTags {
		if _, exists := rollupTags[requiredTag]; !exists {
			return fmt.Errorf("missing required rollup tag: '%s'", requiredTag)
		}
	}

	return nil
}

func validateNoDuplicateRollupIDIn(pipelines []mpipeline.Pipeline) error {
	rollupOps := make([]mpipeline.RollupOp, 0, len(pipelines))
	for _, pipeline := range pipelines {
		numOps := pipeline.Len()
		for i := 0; i < numOps; i++ {
			pipelineOp := pipeline.At(i)
			if pipelineOp.Type != mpipeline.RollupOpType {
				continue
			}
			rollupOp := pipelineOp.Rollup
			for _, existing := range rollupOps {
				if rollupOp.SameTransform(existing) {
					return merrors.NewInvalidInputError(fmt.Sprintf(
						"more than one rollup operations with name '%s' and tags '%s' exist",
						rollupOp.NewName([]byte("")),
						rollupOp.Tags,
					))
				}
			}
			rollupOps = append(rollupOps, rollupOp)
		}
	}
	return nil
}

func (v *validator) wrapError(err error) error {
	if err == nil {
		return nil
	}
	switch err.(type) {
	// Do not wrap error for these error types so caller can take actions
	// based on the correct error type.
	case merrors.InvalidInputError, merrors.ValidationError:
		return err
	default:
		return merrors.NewValidationError(err.Error())
	}
}

type aggregationType int

const (
	// First-level aggregation refers to the aggregation operation performed as the first
	// step of metrics processing, such as the aggregations specified by a mapping rule,
	// or those specified by the first operation in a rollup pipeline.
	firstLevelAggregationType aggregationType = iota

	// Non-first-level aggregation refers to the aggregation operation performed as the
	// second step or later step of a rollup pipeline.
	nonFirstLevelAggregationType
)
