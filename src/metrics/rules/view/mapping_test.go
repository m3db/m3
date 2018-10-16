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

package view

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMappingRuleEqual(t *testing.T) {
	rule1 := MappingRule{
		ID:            "mr_id",
		Name:          "mr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	rule2 := MappingRule{
		ID:            "mr_id",
		Name:          "mr_name",
		CutoverMillis: 1234,
		Filter:        "filter",
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
		},
		LastUpdatedAtMillis: 1234,
		LastUpdatedBy:       "john",
	}
	require.True(t, rule1.Equal(&rule2))
	require.True(t, rule2.Equal(&rule1))
}

func TestMappingRuleNotEqual(t *testing.T) {
	rules := []MappingRule{
		{
			ID:            "mr",
			Name:          "foo",
			Filter:        "filter",
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
			},
		},
		{
			ID:            "mr",
			Name:          "foo",
			Filter:        "filter",
			AggregationID: aggregation.DefaultID,
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
			},
		},
		{
			ID:            "mr",
			Name:          "foo",
			Filter:        "filter",
			AggregationID: aggregation.DefaultID,
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
			},
		},
		{
			ID:            "mr",
			Name:          "bar",
			Filter:        "filter",
			AggregationID: aggregation.DefaultID,
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
			},
		},
		{
			ID:            "mr",
			Name:          "bar",
			Filter:        "filter2",
			AggregationID: aggregation.DefaultID,
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
			},
		},
		{
			ID:            "mr",
			Name:          "foo",
			Filter:        "filter",
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
			},
			DropPolicy: policy.DropMust,
		},
		{
			ID:            "mr",
			Name:          "foo",
			Filter:        "filter",
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
			},
			DropPolicy: policy.DropIfOnlyMatch,
		},
	}
	for i := 0; i < len(rules); i++ {
		for j := i + 1; j < len(rules); j++ {
			require.False(t, rules[i].Equal(&rules[j]))
		}
	}
}

func TestMappingRuleEqualNilCases(t *testing.T) {
	var (
		mr1 *MappingRule
		mr2 MappingRule
	)
	require.True(t, mr1.Equal(nil))
	require.False(t, mr2.Equal(mr1))
}
