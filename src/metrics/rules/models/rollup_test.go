// Copyright (c) 2018 Uber Technologies, Inc.
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

package models

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestRollupTargetToRollupTargetView(t *testing.T) {
	target := &RollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag1"), []byte("tag2")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	res := target.ToRollupTargetView()
	expected := RollupTargetView{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag1"), []byte("tag2")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	require.Equal(t, expected, res)
}

func TestNewRollupTarget(t *testing.T) {
	view := RollupTargetView{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag1"), []byte("tag2")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	res := NewRollupTarget(view)
	expected := RollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag1"), []byte("tag2")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	require.Equal(t, expected, res)
}

func TestRollupTargetEqual(t *testing.T) {
	target1 := RollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag1"), []byte("tag2")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	target2 := RollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:        pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: pipeline.RollupOp{
					NewName:       []byte("name"),
					Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
	}
	require.True(t, target1.Equal(&target2))
}

func TestRollupTargetNotEqual(t *testing.T) {
	targets := []RollupTarget{
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:        pipeline.AggregationOpType,
					Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
				},
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			},
		},
	}

	for i := 0; i < len(targets); i++ {
		for j := i + 1; j < len(targets); j++ {
			require.False(t, targets[i].Equal(&targets[j]))
		}
	}
}

func TestRollupTargetEqualNilCases(t *testing.T) {
	var (
		rt1 *RollupTarget
		rt2 RollupTarget
	)
	require.True(t, rt1.Equal(nil))
	require.False(t, rt2.Equal(rt1))
}

func TestNewRollupRule(t *testing.T) {
	view := RollupRuleView{
		ID:           "rr_id",
		Name:         "rr_name",
		CutoverNanos: 1234000000,
		Filter:       "filter",
		Targets: []RollupTargetView{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtNanos: 1234000000,
		LastUpdatedBy:      "john",
	}
	res := NewRollupRule(&view)

	expected := RollupRule{
		ID:            "rr_id",
		Name:          "rr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		Targets: []RollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	require.Equal(t, expected, res)
}

func TestRollupRuleToRollupRuleView(t *testing.T) {
	rule := RollupRule{
		ID:            "rr_id",
		Name:          "rr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		Targets: []RollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	res := rule.ToRollupRuleView()
	expected := &RollupRuleView{
		ID:           "rr_id",
		Name:         "rr_name",
		CutoverNanos: 1234000000,
		Filter:       "filter",
		Targets: []RollupTargetView{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtNanos: 1234000000,
		LastUpdatedBy:      "john",
	}
	require.Equal(t, expected, res)
}

func TestRollupRuleEqual(t *testing.T) {
	rule1 := RollupRule{
		ID:            "rr_id",
		Name:          "rr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		Targets: []RollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	rule2 := RollupRule{
		ID:            "rr_id",
		Name:          "rr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		Targets: []RollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	require.True(t, rule1.Equal(&rule2))
}

func TestRollupRuleNotEqual(t *testing.T) {
	rules := []RollupRule{
		{
			ID:            "rr_id",
			Name:          "rr_name",
			CutoverMillis: 1234,
			Filter:        "filter",
			Targets: []RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("name"),
								Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
								AggregationID: aggregation.DefaultID,
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
						policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 1234,
			LastUpdatedBy:       "john",
		},
		{
			ID:            "rr_id",
			Name:          "rr_name",
			CutoverMillis: 1234,
			Filter:        "filter2",
			Targets: []RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("name"),
								Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
								AggregationID: aggregation.DefaultID,
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
						policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 1234,
			LastUpdatedBy:       "john",
		},
		{
			ID:            "rr_id",
			Name:          "rr_name",
			CutoverMillis: 1234,
			Filter:        "filter2",
			Targets: []RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
						policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 1234,
			LastUpdatedBy:       "john",
		},
		{
			ID:            "rr_id",
			Name:          "rr_name",
			CutoverMillis: 1234,
			Filter:        "filter2",
			Targets: []RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 1234,
			LastUpdatedBy:       "john",
		},
		{
			ID:            "rr_id2",
			Name:          "rr_name",
			CutoverMillis: 1234,
			Filter:        "filter2",
			Targets: []RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 1234,
			LastUpdatedBy:       "john",
		},
	}

	for i := 0; i < len(rules); i++ {
		for j := i + 1; j < len(rules); j++ {
			require.False(t, rules[i].Equal(&rules[j]))
		}
	}
}

func TestRollupRuleEqualNilCases(t *testing.T) {
	var (
		rr1 *RollupRule
		rr2 RollupRule
	)
	require.True(t, rr1.Equal(nil))
	require.False(t, rr2.Equal(rr1))
}

func TestRollupTargetsEqual(t *testing.T) {
	targets1 := rollupTargets{
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:        pipeline.AggregationOpType,
					Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
				},
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name2"),
						Tags:          [][]byte{[]byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			},
		},
	}
	targets2 := rollupTargets{
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type:        pipeline.AggregationOpType,
					Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
				},
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name"),
						Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: pipeline.RollupOp{
						NewName:       []byte("name2"),
						Tags:          [][]byte{[]byte("tag1")},
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
			},
		},
	}
	require.True(t, targets1.Equal(targets2))
}

func TestRollupTargetsNotEqual(t *testing.T) {
	targetsList := []rollupTargets{
		{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name2"),
							Tags:          [][]byte{[]byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
			},
		},
		{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name2"),
							Tags:          [][]byte{[]byte("tag2")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
			},
		},
		{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:        pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
					},
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("name"),
							Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
	}

	for i := 0; i < len(targetsList); i++ {
		for j := i + 1; j < len(targetsList); j++ {
			require.False(t, targetsList[i].Equal(targetsList[j]))
		}
	}
}

func TestNewRollupRuleSnapshots(t *testing.T) {
	rules := []*RollupRuleView{
		&RollupRuleView{
			ID:           "rr_id",
			Name:         "rr_name",
			CutoverNanos: 1234000000,
			Filter:       "filter",
			Targets: []RollupTargetView{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("name"),
								Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
								AggregationID: aggregation.DefaultID,
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
						policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					},
				},
			},
			LastUpdatedAtNanos: 1234000000,
			LastUpdatedBy:      "john",
		},
		&RollupRuleView{
			ID:           "rr_id2",
			Name:         "rr_name2",
			CutoverNanos: 1234000000,
			Filter:       "filter2",
			Targets: []RollupTargetView{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("name2"),
								Tags:          [][]byte{[]byte("tag3"), []byte("tag4")},
								AggregationID: aggregation.DefaultID,
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					},
				},
			},
			LastUpdatedAtNanos: 1234000000,
			LastUpdatedBy:      "john2",
		},
	}
	res := NewRollupRuleSnapshots(rules)

	expected := RollupRuleSnapshots{
		RollupRules: []RollupRule{
			{
				ID:            "rr_id",
				Name:          "rr_name",
				CutoverMillis: 1234,
				Filter:        "filter",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
				LastUpdatedAtMillis: 1234,
				LastUpdatedBy:       "john",
			},
			{
				ID:            "rr_id2",
				Name:          "rr_name2",
				CutoverMillis: 1234,
				Filter:        "filter2",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name2"),
									Tags:          [][]byte{[]byte("tag3"), []byte("tag4")},
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
				LastUpdatedAtMillis: 1234,
				LastUpdatedBy:       "john2",
			},
		},
	}
	require.Equal(t, expected, res)
}
