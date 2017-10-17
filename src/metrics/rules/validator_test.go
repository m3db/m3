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
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

const (
	testTypeTag     = "type"
	testCounterType = "counter"
	testTimerType   = "timer"
	testGaugeType   = "gauge"
)

func TestValidatorValidateDuplicateMappingRules(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testDuplicateMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	err := ruleSet.Validate(validator)
	require.Error(t, err)
	_, ok := err.(RuleConflictError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateMappingRulesWithTombstone(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testNoDuplicateMappingRulesConfigWithTombstone())
	validator := NewValidator(testValidatorOptions())
	require.NoError(t, ruleSet.Validate(validator))
}

func TestValidatorValidateMappingRuleInvalidFilter(t *testing.T) {
	invalidFilterSnapshot := &mappingRuleSnapshot{
		rawFilters: map[string]string{
			"randomTag": "*too*many*wildcards*",
		},
	}
	invalidFilterRule := &mappingRule{
		snapshots: []*mappingRuleSnapshot{invalidFilterSnapshot},
	}
	rs := &ruleSet{
		mappingRules: []*mappingRule{invalidFilterRule},
	}
	validator := NewValidator(testValidatorOptions())
	require.Error(t, rs.Validate(validator))
}

func TestValidatorValidateMappingRuleInvalidMetricType(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testInvalidMetricTypeMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, ruleSet.Validate(validator))
}

func TestValidatorValidateMappingRulePolicy(t *testing.T) {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:1d"),
	}
	ruleSet := testRuleSetWithMappingRules(t, testPolicyResolutionMappingRulesConfig())

	inputs := []struct {
		opts      ValidatorOptions
		expectErr bool
	}{
		{
			// By default policy is allowed.
			opts:      testValidatorOptions(),
			expectErr: true,
		},
		{
			// Policy is allowed through the default list of policies.
			opts:      testValidatorOptions().SetDefaultAllowedStoragePolicies(testStoragePolicies),
			expectErr: false,
		},
		{
			// Policy is allowed through the list of policies allowed for timers.
			opts:      testValidatorOptions().SetAllowedStoragePoliciesFor(metric.TimerType, testStoragePolicies),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, ruleSet.Validate(validator))
		} else {
			require.NoError(t, ruleSet.Validate(validator))
		}
	}
}

func TestValidatorValidateMappingRuleCustomAggregationTypes(t *testing.T) {
	testAggregationTypes := []policy.AggregationType{policy.Count, policy.Max}
	ruleSet := testRuleSetWithMappingRules(t, testCustomAggregationTypeMappingRulesConfig())
	inputs := []struct {
		opts      ValidatorOptions
		expectErr bool
	}{
		{
			// By default no custom aggregation type is allowed.
			opts:      testValidatorOptions(),
			expectErr: true,
		},
		{
			// Aggregation type is allowed through the default list of custom aggregation types.
			opts:      testValidatorOptions().SetDefaultAllowedCustomAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Aggregation type is allowed through the list of custom aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedCustomAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, ruleSet.Validate(validator))
		} else {
			require.NoError(t, ruleSet.Validate(validator))
		}
	}
}

func TestValidatorValidateDuplicateRollupRules(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testDuplicateRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	err := ruleSet.Validate(validator)
	require.Error(t, err)
	_, ok := err.(RuleConflictError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateRollupRulesWithTombstone(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testNoDuplicateRollupRulesConfigWithTombstone())
	validator := NewValidator(testValidatorOptions())
	require.NoError(t, ruleSet.Validate(validator))
}

func TestValidatorValidateRollupRuleInvalidFilter(t *testing.T) {
	invalidFilterSnapshot := &rollupRuleSnapshot{
		rawFilters: map[string]string{
			"randomTag": "*too*many*wildcards*",
		},
	}
	invalidFilterRule := &rollupRule{
		snapshots: []*rollupRuleSnapshot{invalidFilterSnapshot},
	}
	rs := &ruleSet{
		rollupRules: []*rollupRule{invalidFilterRule},
	}
	validator := NewValidator(testValidatorOptions())
	require.Error(t, rs.Validate(validator))
}

func TestValidatorValidateRollupRuleInvalidMetricType(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testInvalidMetricTypeRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, ruleSet.Validate(validator))
}

func TestValidatorValidateRollupRuleMissingRequiredTag(t *testing.T) {
	requiredRollupTags := []string{"requiredTag"}
	ruleSet := testRuleSetWithRollupRules(t, testMissingRequiredTagRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetRequiredRollupTags(requiredRollupTags))
	require.Error(t, ruleSet.Validate(validator))
}

func TestValidatorValidateRollupRulePolicy(t *testing.T) {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:1d"),
	}
	ruleSet := testRuleSetWithRollupRules(t, testPolicyResolutionRollupRulesConfig())

	inputs := []struct {
		opts      ValidatorOptions
		expectErr bool
	}{
		{
			// By default policy is allowed.
			opts:      testValidatorOptions(),
			expectErr: true,
		},
		{
			// Policy is allowed through the default list of policies.
			opts:      testValidatorOptions().SetDefaultAllowedStoragePolicies(testStoragePolicies),
			expectErr: false,
		},
		{
			// Policy is allowed through the list of policies allowed for timers.
			opts:      testValidatorOptions().SetAllowedStoragePoliciesFor(metric.TimerType, testStoragePolicies),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, ruleSet.Validate(validator))
		} else {
			require.NoError(t, ruleSet.Validate(validator))
		}
	}
}

func TestValidatorValidateRollupRuleCustomAggregationTypes(t *testing.T) {
	testAggregationTypes := []policy.AggregationType{policy.Count, policy.Max}
	ruleSet := testRuleSetWithRollupRules(t, testCustomAggregationTypeRollupRulesConfig())
	inputs := []struct {
		opts      ValidatorOptions
		expectErr bool
	}{
		{
			// By default no custom aggregation type is allowed.
			opts:      testValidatorOptions(),
			expectErr: true,
		},
		{
			// Aggregation type is allowed through the default list of custom aggregation types.
			opts:      testValidatorOptions().SetDefaultAllowedCustomAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Aggregation type is allowed through the list of custom aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedCustomAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, ruleSet.Validate(validator))
		} else {
			require.NoError(t, ruleSet.Validate(validator))
		}
	}
}

func TestValidatorValidateRollupRuleConflictingTargets(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testConflictingTargetsRollupRulesConfig())
	opts := testValidatorOptions()
	validator := NewValidator(opts)
	err := ruleSet.Validate(validator)
	require.Error(t, err)
	_, ok := err.(RuleConflictError)
	require.True(t, ok)
}

func testDuplicateMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
	}
}

func testNoDuplicateMappingRulesConfigWithTombstone() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: true,
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
	}
}

func testInvalidMetricTypeMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: "nonexistent",
					},
				},
			},
		},
	}
}

func testPolicyResolutionMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(24 * time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}
}

func testCustomAggregationTypeMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(6 * time.Hour),
								},
							},
							AggregationTypes: []schema.AggregationType{
								schema.AggregationType_COUNT,
								schema.AggregationType_MAX,
							},
						},
					},
				},
			},
		},
	}
}

func testDuplicateRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
	}
}

func testNoDuplicateRollupRulesConfigWithTombstone() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: true,
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
				},
			},
		},
	}
}

func testInvalidMetricTypeRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: "nonexistent",
					},
				},
			},
		},
	}
}

func testMissingRequiredTagRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
						},
					},
				},
			},
		},
	}
}

func testPolicyResolutionRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(10 * time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(24 * time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func testCustomAggregationTypeRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(10 * time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(6 * time.Hour),
										},
									},
									AggregationTypes: []schema.AggregationType{
										schema.AggregationType_COUNT,
										schema.AggregationType_MAX,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func testConflictingTargetsRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(10 * time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(6 * time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot2",
					Tombstoned: false,
					TagFilters: map[string]string{
						testTypeTag: testTimerType,
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(10 * time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(6 * time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func testRuleSetWithMappingRules(t *testing.T, mrs []*schema.MappingRule) RuleSet {
	rs := &schema.RuleSet{MappingRules: mrs}
	newRuleSet, err := NewRuleSetFromSchema(1, rs, testRuleSetOptions())
	require.NoError(t, err)
	return newRuleSet
}

func testRuleSetWithRollupRules(t *testing.T, rrs []*schema.RollupRule) RuleSet {
	rs := &schema.RuleSet{RollupRules: rrs}
	newRuleSet, err := NewRuleSetFromSchema(1, rs, testRuleSetOptions())
	require.NoError(t, err)
	return newRuleSet
}

func testValidatorOptions() ValidatorOptions {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:6h"),
	}
	return NewValidatorOptions().
		SetDefaultAllowedStoragePolicies(testStoragePolicies).
		SetDefaultAllowedCustomAggregationTypes(nil).
		SetMetricTypesFn(testMetricTypesFn())
}

func testMetricTypesFn() MetricTypesFn {
	return func(filters map[string]string) ([]metric.Type, error) {
		typ, exists := filters[testTypeTag]
		if !exists {
			return []metric.Type{metric.UnknownType}, nil
		}
		switch typ {
		case testCounterType:
			return []metric.Type{metric.CounterType}, nil
		case testTimerType:
			return []metric.Type{metric.TimerType}, nil
		case testGaugeType:
			return []metric.Type{metric.GaugeType}, nil
		default:
			return nil, fmt.Errorf("unknown metric type %v", typ)
		}
	}
}
