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
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/models"
	"github.com/m3db/m3metrics/rules/validator/namespace"
	"github.com/m3db/m3metrics/rules/validator/namespace/kv"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

const (
	testTypeTag       = "type"
	testCounterType   = "counter"
	testTimerType     = "timer"
	testGaugeType     = "gauge"
	testNamespacesKey = "testNamespaces"
)

var (
	testNamespaces = []string{"foo", "bar"}
)

func TestValidatorDefaultNamespaceValidator(t *testing.T) {
	v := NewValidator(testValidatorOptions()).(*validator)

	inputs := []string{"foo", "bar", "baz"}
	for _, input := range inputs {
		require.NoError(t, v.validateNamespace(input))
	}
}

func TestValidatorInvalidNamespace(t *testing.T) {
	defer leaktest.Check(t)()

	nsValidator := testKVNamespaceValidator(t)
	opts := testValidatorOptions().SetNamespaceValidator(nsValidator)
	v := NewValidator(opts)
	defer v.Close()

	rs, err := rules.NewRuleSetFromSchema(1, &schema.RuleSet{Namespace: "baz"}, rules.NewOptions())
	require.NoError(t, err)
	require.Error(t, v.Validate(rs))
}

func TestValidatorValidNamespace(t *testing.T) {
	defer leaktest.Check(t)()

	nsValidator := testKVNamespaceValidator(t)
	opts := testValidatorOptions().SetNamespaceValidator(nsValidator)
	v := NewValidator(opts)
	defer v.Close()

	rs, err := rules.NewRuleSetFromSchema(1, &schema.RuleSet{Namespace: "foo"}, rules.NewOptions())
	require.NoError(t, err)
	require.NoError(t, v.Validate(rs))
}

func TestValidatorValidateDuplicateMappingRules(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testDuplicateMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	err := validator.Validate(ruleSet)
	require.Error(t, err)
	_, ok := err.(errors.InvalidInputError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateMappingRulesWithTombstone(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testNoDuplicateMappingRulesConfigWithTombstone())
	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.Validate(ruleSet))
}

func TestValidatorValidateMappingRuleInvalidFilterExpr(t *testing.T) {
	snapshot := testInvalidFilterExprMappingRuleSetSnapshot()
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(snapshot))
}

func TestValidatorValidateMappingRuleInvalidFilterTagName(t *testing.T) {
	invalidChars := []rune{'$'}
	snapshot := testInvalidFilterTagNameMappingRuleSetSnapshot()
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(snapshot))
}

func TestValidatorValidateMappingRuleInvalidMetricType(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testInvalidMetricTypeMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateMappingRulePolicy(t *testing.T) {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:1d"),
	}
	ruleSet := testRuleSetWithMappingRules(t, testPolicyResolutionMappingRulesConfig())

	inputs := []struct {
		opts      Options
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
			require.Error(t, validator.Validate(ruleSet))
		} else {
			require.NoError(t, validator.Validate(ruleSet))
		}
	}
}

func TestValidatorValidateMappingRuleNoPolicies(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testNoPoliciesMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateMappingRuleDuplicatePolicies(t *testing.T) {
	ruleSet := testRuleSetWithMappingRules(t, testDuplicatePoliciesMappingRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateMappingRuleCustomAggregationTypes(t *testing.T) {
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	ruleSet := testRuleSetWithMappingRules(t, testCustomAggregationTypeMappingRulesConfig())
	inputs := []struct {
		opts      Options
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
			require.Error(t, validator.Validate(ruleSet))
		} else {
			require.NoError(t, validator.Validate(ruleSet))
		}
	}
}

func TestValidatorValidateDuplicateRollupRules(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testDuplicateRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	err := validator.Validate(ruleSet)
	require.Error(t, err)
	_, ok := err.(errors.InvalidInputError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateRollupRulesWithTombstone(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testNoDuplicateRollupRulesConfigWithTombstone())
	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleInvalidFilterExpr(t *testing.T) {
	snapshot := testInvalidFilterExprRollupRuleSetSnapshot()
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(snapshot))
}

func TestValidatorValidateRollupRuleInvalidFilterTagName(t *testing.T) {
	invalidChars := []rune{'$'}
	snapshot := testInvalidFilterTagNameRollupRuleSetSnapshot()
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(snapshot))
}

func TestValidatorValidateRollupRuleInvalidMetricType(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testInvalidMetricTypeRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleDuplicateRollupTag(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testDuplicateTagRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleMissingRequiredTag(t *testing.T) {
	requiredRollupTags := []string{"requiredTag"}
	ruleSet := testRuleSetWithRollupRules(t, testMissingRequiredTagRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetRequiredRollupTags(requiredRollupTags))
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidateChars(t *testing.T) {
	invalidChars := map[rune]struct{}{
		'$': struct{}{},
	}
	require.Error(t, validateChars("test$", invalidChars))
	require.NoError(t, validateChars("test", invalidChars))
}

func TestValidatorValidateRollupRuleWithInvalidMetricName(t *testing.T) {
	invalidChars := []rune{'$'}
	ruleSet := testRuleSetWithRollupRules(t, testMetricNameRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleWithEmptyMetricName(t *testing.T) {
	invalidChars := []rune{'$'}
	ruleSet := testRuleSetWithRollupRules(t, testEmptyMetricNameRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleWithValidMetricName(t *testing.T) {
	invalidChars := []rune{' ', '%'}
	ruleSet := testRuleSetWithRollupRules(t, testMetricNameRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.NoError(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleWithInvalidTagName(t *testing.T) {
	invalidChars := []rune{'$'}
	ruleSet := testRuleSetWithRollupRules(t, testTagNameRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleWithValidTagName(t *testing.T) {
	invalidChars := []rune{' ', '%'}
	ruleSet := testRuleSetWithRollupRules(t, testTagNameRollupRulesConfig())
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.NoError(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRulePolicy(t *testing.T) {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:1d"),
	}
	ruleSet := testRuleSetWithRollupRules(t, testPolicyResolutionRollupRulesConfig())

	inputs := []struct {
		opts      Options
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
			require.Error(t, validator.Validate(ruleSet))
		} else {
			require.NoError(t, validator.Validate(ruleSet))
		}
	}
}

func TestValidatorValidateRollupRuleWithNoPolicies(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testNoPoliciesRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleWithDuplicatePolicies(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testDuplicatePoliciesRollupRulesConfig())
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.Validate(ruleSet))
}

func TestValidatorValidateRollupRuleCustomAggregationTypes(t *testing.T) {
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	ruleSet := testRuleSetWithRollupRules(t, testCustomAggregationTypeRollupRulesConfig())
	inputs := []struct {
		opts      Options
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
			require.Error(t, validator.Validate(ruleSet))
		} else {
			require.NoError(t, validator.Validate(ruleSet))
		}
	}
}

func TestValidatorValidateRollupRuleConflictingTargets(t *testing.T) {
	ruleSet := testRuleSetWithRollupRules(t, testConflictingTargetsRollupRulesConfig())
	opts := testValidatorOptions()
	validator := NewValidator(opts)
	err := validator.Validate(ruleSet)
	require.Error(t, err)
	_, ok := err.(errors.InvalidInputError)
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
					Policies:   testPolicies(),
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
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
					Policies:   testPolicies(),
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Policies:   testPolicies(),
				},
			},
		},
	}
}

func testInvalidFilterExprMappingRuleSetSnapshot() *models.RuleSetSnapshotView {
	return &models.RuleSetSnapshotView{
		MappingRules: map[string]*models.MappingRuleView{
			"mappingRule1": &models.MappingRuleView{
				Name:   "snapshot1",
				Filter: "randomTag:*too*many*wildcards*",
			},
		},
	}
}

func testInvalidFilterTagNameMappingRuleSetSnapshot() *models.RuleSetSnapshotView {
	return &models.RuleSetSnapshotView{
		MappingRules: map[string]*models.MappingRuleView{
			"mappingRule1": &models.MappingRuleView{
				Name:   "snapshot1",
				Filter: "random$Tag:foo",
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
					Filter:     testTypeTag + ":nonexistent",
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
					Filter:     testTypeTag + ":" + testTimerType,
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

func testNoPoliciesMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Filter:     testTypeTag + ":" + testTimerType,
					Policies:   []*schema.Policy{},
				},
			},
		},
	}
}

func testDuplicatePoliciesMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Filter:     testTypeTag + ":" + testTimerType,
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
					Filter:     testTypeTag + ":" + testTimerType,
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
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
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
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
	}
}

func testInvalidFilterExprRollupRuleSetSnapshot() *models.RuleSetSnapshotView {
	return &models.RuleSetSnapshotView{
		RollupRules: map[string]*models.RollupRuleView{
			"rollupRule1": &models.RollupRuleView{
				Name:   "snapshot1",
				Filter: "randomTag:*too*many*wildcards*",
			},
		},
	}
}

func testInvalidFilterTagNameRollupRuleSetSnapshot() *models.RuleSetSnapshotView {
	return &models.RuleSetSnapshotView{
		RollupRules: map[string]*models.RollupRuleView{
			"rollupRule1": &models.RollupRuleView{
				Name:   "snapshot1",
				Filter: "random$Tag:foo",
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
					Filter:     testTypeTag + ":nonexistent",
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
	}
}

func testDuplicateTagRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2", "rtagName1"},
							Policies: testPolicies(),
						},
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
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
	}
}

func testTagNameRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2$", "$"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
	}
}

func testMetricNameRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName$1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: testPolicies(),
						},
					},
				},
			},
		},
	}
}

func testEmptyMetricNameRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "",
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
					Filter:     testTypeTag + ":" + testTimerType,
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

func testNoPoliciesRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Filter:     testTypeTag + ":" + testTimerType,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name:     "rName1",
							Tags:     []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{},
						},
					},
				},
			},
		},
	}
}

func testDuplicatePoliciesRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:       "snapshot1",
					Tombstoned: false,
					Filter:     testTypeTag + ":" + testTimerType,
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
					Filter:     testTypeTag + ":" + testTimerType,
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
					Filter:     testTypeTag + ":" + testTimerType,
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
					Filter:     testTypeTag + ":" + testTimerType,
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName2", "rtagName1"},
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

func testRuleSetWithMappingRules(t *testing.T, mrs []*schema.MappingRule) rules.RuleSet {
	rs := &schema.RuleSet{MappingRules: mrs}
	newRuleSet, err := rules.NewRuleSetFromSchema(1, rs, rules.NewOptions())
	require.NoError(t, err)
	return newRuleSet
}

func testRuleSetWithRollupRules(t *testing.T, rrs []*schema.RollupRule) rules.RuleSet {
	rs := &schema.RuleSet{RollupRules: rrs}
	newRuleSet, err := rules.NewRuleSetFromSchema(1, rs, rules.NewOptions())
	require.NoError(t, err)
	return newRuleSet
}

func testKVNamespaceValidator(t *testing.T) namespace.Validator {
	store := mem.NewStore()
	_, err := store.Set(testNamespacesKey, &commonpb.StringArrayProto{Values: testNamespaces})
	require.NoError(t, err)
	kvOpts := kv.NewNamespaceValidatorOptions().
		SetKVStore(store).
		SetValidNamespacesKey(testNamespacesKey)
	nsValidator, err := kv.NewNamespaceValidator(kvOpts)
	require.NoError(t, err)
	return nsValidator
}

func testValidatorOptions() Options {
	testStoragePolicies := []policy.StoragePolicy{
		policy.MustParseStoragePolicy("10s:6h"),
	}
	return NewOptions().
		SetDefaultAllowedStoragePolicies(testStoragePolicies).
		SetDefaultAllowedCustomAggregationTypes(nil).
		SetMetricTypesFn(testMetricTypesFn())
}

func testMetricTypesFn() MetricTypesFn {
	return func(filters filters.TagFilterValueMap) ([]metric.Type, error) {
		fv, exists := filters[testTypeTag]
		if !exists {
			return []metric.Type{metric.UnknownType}, nil
		}
		switch fv.Pattern {
		case testCounterType:
			return []metric.Type{metric.CounterType}, nil
		case testTimerType:
			return []metric.Type{metric.TimerType}, nil
		case testGaugeType:
			return []metric.Type{metric.GaugeType}, nil
		default:
			return nil, fmt.Errorf("unknown metric type %v", fv.Pattern)
		}
	}
}

func testPolicies() []*schema.Policy {
	return []*schema.Policy{
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
	}
}
