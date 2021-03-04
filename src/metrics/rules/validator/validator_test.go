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
	"math"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/validator/namespace"
	"github.com/m3db/m3/src/metrics/rules/validator/namespace/kv"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/transformation"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
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

	view := view.RuleSet{Namespace: "baz"}
	require.Error(t, v.ValidateSnapshot(view))
}

func TestValidatorValidNamespace(t *testing.T) {
	defer leaktest.Check(t)()

	nsValidator := testKVNamespaceValidator(t)
	opts := testValidatorOptions().SetNamespaceValidator(nsValidator)
	v := NewValidator(opts)
	defer v.Close()

	view := view.RuleSet{Namespace: "foo"}
	require.NoError(t, v.ValidateSnapshot(view))
}

func TestValidatorValidateDuplicateMappingRules(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          "tag1:value1",
				StoragePolicies: testStoragePolicies(),
			},
			{
				Name:            "snapshot1",
				Filter:          "tag1:value1",
				StoragePolicies: testStoragePolicies(),
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	_, ok := err.(errors.InvalidInputError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateMappingRulesWithTombstone(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          "tag1:value1",
				Tombstoned:      true,
				StoragePolicies: testStoragePolicies(),
			},
			{
				Name:            "snapshot1",
				Filter:          "tag1:value1",
				StoragePolicies: testStoragePolicies(),
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleInvalidFilterExpr(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:   "snapshot1",
				Filter: "randomTag:*too*many*wildcards*",
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleInvalidFilterTagName(t *testing.T) {
	invalidChars := []rune{'$'}
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:   "snapshot1",
				Filter: "random$Tag:foo",
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleInvalidMetricType(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          testTypeTag + ":nonexistent",
				StoragePolicies: testStoragePolicies(),
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleInvalidAggregationType(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:          "snapshot1",
				Filter:        testTypeTag + ":" + testCounterType,
				AggregationID: aggregation.ID{1234567789},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleMultipleAggregationTypes(t *testing.T) {
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          testTypeTag + ":" + testTimerType,
				AggregationID:   aggregation.MustCompressTypes(aggregation.Count, aggregation.Max),
				StoragePolicies: testStoragePolicies(),
			},
		},
	}
	inputs := []struct {
		opts      Options
		expectErr bool
	}{
		{
			// By default multiple aggregation types are allowed for timers.
			opts:      testValidatorOptions().SetAllowedFirstLevelAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
		{
			// Explicitly disallow multiple aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedFirstLevelAggregationTypesFor(metric.TimerType, testAggregationTypes).SetMultiAggregationTypesEnabledFor(nil),
			expectErr: true,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateMappingRuleFirstLevelAggregationType(t *testing.T) {
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          testTypeTag + ":" + testTimerType,
				AggregationID:   aggregation.MustCompressTypes(aggregation.Count, aggregation.Max),
				StoragePolicies: testStoragePolicies(),
			},
		},
	}
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
			opts:      testValidatorOptions().SetDefaultAllowedFirstLevelAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Aggregation type is allowed through the list of custom aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedFirstLevelAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateMappingRuleNoStoragePolicies(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no storage policies"))
}

func TestValidatorValidateMappingRuleDuplicateStoragePolicies(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				StoragePolicies: policy.StoragePolicies{
					policy.MustParseStoragePolicy("10s:6h"),
					policy.MustParseStoragePolicy("10s:6h"),
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "duplicate storage policy '10s:6h'"))
}

func TestValidatorValidateMappingRuleDisallowedStoragePolicies(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				StoragePolicies: policy.StoragePolicies{
					policy.MustParseStoragePolicy("1s:6h"),
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRule(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:            "snapshot1",
				Filter:          testTypeTag + ":" + testCounterType,
				StoragePolicies: testStoragePolicies(),
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateDuplicateRollupRules(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: "tag1:value1",
			},
			{
				Name:   "snapshot1",
				Filter: "tag1:value1",
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	_, ok := err.(errors.InvalidInputError)
	require.True(t, ok)
}

func TestValidatorValidateNoDuplicateRollupRulesWithTombstone(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:       "snapshot1",
				Filter:     "tag1:value1",
				Tombstoned: true,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
			{
				Name:   "snapshot1",
				Filter: "tag1:value1",
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleInvalidFilterExpr(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: "randomTag:*too*many*wildcards*",
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleInvalidFilterTagName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	invalidChars := []rune{'$'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: "random$Tag:foo",
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleInvalidMetricType(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":nonexistent",
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRulePipelineEmptyPipeline(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline:        pipeline.NewPipeline([]pipeline.OpUnion{}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "empty pipeline"))
}

func TestValidatorValidateRollupRulePipelineInvalidPipelineOp(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "operation at index 0 has invalid type"))
}

func TestValidatorValidateRollupRulePipelineMultipleAggregationOps(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	allowedAggregationTypes := aggregation.Types{aggregation.Sum}
	opts := testValidatorOptions().
		SetAllowedFirstLevelAggregationTypesFor(metric.CounterType, allowedAggregationTypes)
	validator := NewValidator(opts)
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "more than one aggregation operation in pipeline"))
}

func TestValidatorValidateRollupRulePipelineAggregationOpNotFirst(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	allowedAggregationTypes := aggregation.Types{aggregation.Sum}
	opts := testValidatorOptions().
		SetAllowedFirstLevelAggregationTypesFor(metric.CounterType, allowedAggregationTypes)
	validator := NewValidator(opts)
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "aggregation operation is not the first operation in pipeline"))
}

func TestValidatorValidateRollupRulePipelineAggregationOpInvalidAggregationType(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	allowedAggregationTypes := aggregation.Types{aggregation.Sum}
	opts := testValidatorOptions().
		SetAllowedFirstLevelAggregationTypesFor(metric.CounterType, allowedAggregationTypes)
	validator := NewValidator(opts)
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation operation at index 0"))
}

func TestValidatorValidateRollupRulePipelineAggregationOpDisallowedAggregationType(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation operation at index 0"))
}

func TestValidatorValidateRollupRulePipelineTransformationDerivativeOrderNotSupported(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "transformation derivative order is 2 higher than supported 1"))
}

func TestValidatorValidateRollupRulePipelineInvalidTransformationType(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid transformation operation at index 0"))
}

func TestValidatorValidateRollupRulePipelineNoRollupOp(t *testing.T) {
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err := validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no rollup operation in pipeline"))
}

func TestValidatorValidateRollupRulePipelineRollupLevelHigherThanMax(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "number of rollup levels is 2 higher than supported 1"))
}

func TestValidatorValidateRollupRulePipelineRollupTagNotFoundInPrevRollupOp(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2", "rtagName3"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetMaxRollupLevels(100))
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tag rtagName3 not found in previous rollup operations"))
}

func TestValidatorValidateRollupRulePipelineRollupTagUnchangedInConsecutiveRollupOps(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetMaxRollupLevels(100))
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "same set of 2 rollup tags in consecutive rollup operations"))
}

func TestValidatorValidateRollupRulePipelineMultiLevelRollup(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2", "rtagName3"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr3, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr3,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetMaxRollupLevels(3))
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleRollupOpDuplicateRollupTag(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "duplicate rollup tag: 'rtagName2'"))
}

func TestValidatorValidateRollupRuleRollupOpMissingRequiredTag(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions().SetRequiredRollupTags([]string{"requiredTag"}))
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "missing required rollup tag: 'requiredTag'"))
}

func TestValidatorValidateRollupRuleRollupOpWithInvalidMetricName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName$1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	invalidChars := []rune{'$'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleRollupOpWithEmptyMetricName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	invalidChars := []rune{'$'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleRollupOpWithValidMetricName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	invalidChars := []rune{' ', '%'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleRollupOpWithInvalidTagName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"foo",
		[]string{"rtagName1", "rtagName2", "$"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	invalidChars := []rune{'$'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions().SetTagNameInvalidChars(invalidChars))
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleRollupOpWithValidTagName(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"foo",
		[]string{"rtagName1", "rtagName2", "$"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	invalidChars := []rune{' ', '%'}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testCounterType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions().SetMetricNameInvalidChars(invalidChars))
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateNoTimertypeFilter(t *testing.T) {
	for _, test := range []string{
		"rollup",
		"mapping",
	} {
		t.Run(test, func(t *testing.T) {
			ruleView := view.RuleSet{}
			if test == "rollup" {
				ruleView.RollupRules = []view.RollupRule{
					{
						Name:   "foo",
						Filter: "service:bar timertype:count",
					},
				}
			} else {
				ruleView.MappingRules = []view.MappingRule{
					{
						Name:       "foo",
						Filter:     "service:bar timertype:count",
						DropPolicy: policy.DropMust,
					},
				}
			}

			validator := NewValidator(testValidatorOptions().SetFilterInvalidTagNames([]string{"timertype"}))
			assert.Error(t, validator.ValidateSnapshot(ruleView))

			if test == "rollup" {
				ruleView.RollupRules = ruleView.RollupRules[:0]
			} else {
				ruleView.MappingRules = ruleView.MappingRules[:0]
			}

			assert.NoError(t, validator.ValidateSnapshot(ruleView))
		})
	}
}

func TestValidatorValidateRollupRuleRollupOpMultipleAggregationTypes(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Count, aggregation.Max),
	)
	require.NoError(t, err)

	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("10s:6h"),
						},
					},
				},
			},
		},
	}
	inputs := []struct {
		opts      Options
		expectErr bool
	}{
		{
			// By default multiple aggregation types are allowed for timers.
			opts:      testValidatorOptions().SetDefaultAllowedFirstLevelAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Explicitly disallow multiple aggregation types for timers.
			opts:      testValidatorOptions().SetDefaultAllowedFirstLevelAggregationTypes(testAggregationTypes).SetMultiAggregationTypesEnabledFor(nil),
			expectErr: true,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateRollupRuleRollupOpFirstLevelAggregationTypes(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Count, aggregation.Max),
	)
	require.NoError(t, err)
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("10s:6h"),
						},
					},
				},
			},
		},
	}
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
			opts:      testValidatorOptions().SetDefaultAllowedFirstLevelAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Aggregation type is allowed through the list of custom aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedFirstLevelAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateRollupRuleRollupOpNonFirstLevelAggregationTypes(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Count, aggregation.Max),
	)
	require.NoError(t, err)
	testAggregationTypes := []aggregation.Type{aggregation.Count, aggregation.Max}
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("10s:6h"),
						},
					},
				},
			},
		},
	}
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
			opts:      testValidatorOptions().SetDefaultAllowedNonFirstLevelAggregationTypes(testAggregationTypes),
			expectErr: false,
		},
		{
			// Aggregation type is allowed through the list of non-first-level aggregation types for timers.
			opts:      testValidatorOptions().SetAllowedNonFirstLevelAggregationTypesFor(metric.TimerType, testAggregationTypes),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateRollupRuleRollupTargetWithStoragePolicies(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	storagePolicies := testStoragePolicies()
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: storagePolicies,
					},
				},
			},
		},
	}

	inputs := []struct {
		opts      Options
		expectErr bool
	}{
		{
			// By default policy is allowed.
			opts:      testValidatorOptions().SetDefaultAllowedStoragePolicies(policy.StoragePolicies{}),
			expectErr: true,
		},
		{
			// Policy is allowed through the default list of policies.
			opts:      testValidatorOptions().SetDefaultAllowedStoragePolicies(storagePolicies),
			expectErr: false,
		},
		{
			// Policy is allowed through the list of policies allowed for timers.
			opts:      testValidatorOptions().SetAllowedStoragePoliciesFor(metric.TimerType, storagePolicies),
			expectErr: false,
		},
	}

	for _, input := range inputs {
		validator := NewValidator(input.opts)
		if input.expectErr {
			require.Error(t, validator.ValidateSnapshot(view))
		} else {
			require.NoError(t, validator.ValidateSnapshot(view))
		}
	}
}

func TestValidatorValidateRollupRuleRollupTargetWithNoStoragePolicies(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
					},
				},
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no storage policies"))
}

func TestValidatorValidateRollupRuleRollupOpWithDuplicateStoragePolicies(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("10s:6h"),
							policy.MustParseStoragePolicy("10s:6h"),
						},
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "duplicate storage policy '10s:6h'"))
}

func TestValidatorValidateRollupRuleDisallowedStoragePolicies(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testTimerType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1s:6h"),
						},
					},
				},
			},
		},
	}

	validator := NewValidator(testValidatorOptions())
	require.Error(t, validator.ValidateSnapshot(view))
}

func TestValidatorRollupRule(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Sum),
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testGaugeType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Last},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}

	firstLevelAggregationTypes := aggregation.Types{aggregation.Last}
	nonFirstLevelAggregationTypes := aggregation.Types{aggregation.Sum}
	opts := testValidatorOptions().
		SetAllowedFirstLevelAggregationTypesFor(metric.GaugeType, firstLevelAggregationTypes).
		SetAllowedNonFirstLevelAggregationTypesFor(metric.GaugeType, nonFirstLevelAggregationTypes)
	validator := NewValidator(opts)
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateRollupRuleDuplicateRollupIDs(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Sum),
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	view := view.RuleSet{
		RollupRules: []view.RollupRule{
			{
				Name:   "snapshot1",
				Filter: testTypeTag + ":" + testGaugeType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Last},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
			{
				Name:   "snapshot2",
				Filter: testTypeTag + ":" + testGaugeType,
				Targets: []view.RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: testStoragePolicies(),
					},
				},
			},
		},
	}
	firstLevelAggregationTypes := aggregation.Types{aggregation.Last}
	nonFirstLevelAggregationTypes := aggregation.Types{aggregation.Sum}
	opts := testValidatorOptions().
		SetAllowedFirstLevelAggregationTypesFor(metric.GaugeType, firstLevelAggregationTypes).
		SetAllowedNonFirstLevelAggregationTypesFor(metric.GaugeType, nonFirstLevelAggregationTypes)
	validator := NewValidator(opts)
	err = validator.ValidateSnapshot(view)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "more than one rollup operations with name 'rName1' and tags '[rtagName1 rtagName2]' exist"))
	_, ok := err.(errors.InvalidInputError)
	require.True(t, ok)
}

func TestValidatorValidateMappingRuleValidDropPolicy(t *testing.T) {
	view := view.RuleSet{
		MappingRules: []view.MappingRule{
			{
				Name:       "snapshot1",
				Filter:     "tag1:value1",
				DropPolicy: policy.DropMust,
			},
		},
	}
	validator := NewValidator(testValidatorOptions())
	require.NoError(t, validator.ValidateSnapshot(view))
}

func TestValidatorValidateMappingRuleInvalidDropPolicy(t *testing.T) {
	type invalidDropPolicyTest struct {
		name string
		view view.RuleSet
	}

	tests := []invalidDropPolicyTest{
		{
			name: "invalid drop policy",
			view: view.RuleSet{
				MappingRules: []view.MappingRule{
					{
						Name:       "snapshot1",
						Filter:     "tag1:value1",
						DropPolicy: policy.DropPolicy(math.MaxUint32),
					},
				},
			},
		},
	}

	for _, dropPolicy := range policy.ValidDropPolicies() {
		if dropPolicy == policy.DropNone {
			continue // The drop none policy is always valid, since its not active
		}

		tests = append(tests, []invalidDropPolicyTest{
			{
				name: dropPolicy.String() + " policy with storage policies",
				view: view.RuleSet{
					MappingRules: []view.MappingRule{
						{
							Name:            "snapshot1",
							Filter:          "tag1:value1",
							DropPolicy:      policy.DropMust,
							StoragePolicies: testStoragePolicies(),
						},
					},
				},
			},
			{
				name: dropPolicy.String() + " policy with non-default aggregation ID",
				view: view.RuleSet{
					MappingRules: []view.MappingRule{
						{
							Name:       "snapshot1",
							Filter:     "tag1:value1",
							DropPolicy: policy.DropMust,
							AggregationID: aggregation.NewIDCompressor().MustCompress(
								aggregation.Types{aggregation.Last},
							),
						},
					},
				},
			},
		}...)
	}

	validator := NewValidator(testValidatorOptions())

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, validator.ValidateSnapshot(test.view))
		})
	}
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

func testStoragePolicies() policy.StoragePolicies {
	return policy.StoragePolicies{
		policy.MustParseStoragePolicy("10s:6h"),
		policy.MustParseStoragePolicy("1m:24h"),
	}
}

func testValidatorOptions() Options {
	return NewOptions().
		SetDefaultAllowedStoragePolicies(testStoragePolicies()).
		SetDefaultAllowedFirstLevelAggregationTypes(nil).
		SetMetricTypesFn(testMetricTypesFn()).
		SetMultiAggregationTypesEnabledFor([]metric.Type{metric.TimerType})
}
