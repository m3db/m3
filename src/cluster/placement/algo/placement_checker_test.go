// Copyright (c) 2021 Uber Technologies, Inc.
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

package algo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type placementCheckerTest struct {
	name     string
	globalFn func(placement.Placement, []string, int64) bool
	localFn  func(placement.Placement, []string, int64) bool
	cases    []placementCheckerTestCase
}

type placementCheckerTestCase struct {
	instances      []string
	expectedGlobal bool
	expectedLocal  bool
}

//nolint:dupl
func TestPlacementChecker(t *testing.T) {
	var nowNanos int64 = 10
	pastNanos := nowNanos - 1
	futureNanos := nowNanos + 1

	i1 := newTestInstance("i1").SetShards(newTestShards(shard.Available, 1, 4, 0, pastNanos))
	i2 := newTestInstance("i2").SetShards(newTestShards(shard.Available, 1, 4, 0, pastNanos))
	i3 := newTestInstance("i3").SetShards(newTestShards(shard.Leaving, 5, 8, futureNanos, 0))
	i4 := newTestInstance("i4").SetShards(newTestShards(shard.Leaving, 5, 8, futureNanos, 0))
	i5 := newTestInstance("i5").SetShards(newTestShards(shard.Initializing, 9, 12, 0, futureNanos))
	i6 := newTestInstance("i6").SetShards(newTestShards(shard.Initializing, 9, 12, 0, futureNanos))
	p := placement.NewPlacement().SetInstances([]placement.Instance{i1, i2, i3, i4, i5, i6})

	tests := []placementCheckerTest{
		{
			name:     "allAvailable",
			globalFn: globalChecker.allAvailable,
			localFn:  localChecker.allAvailable,
			cases: []placementCheckerTestCase{
				{
					instances:      []string{"i1", "i2"},
					expectedGlobal: true,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i1"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i2"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i3"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"i1,i3"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"non-existent"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{},
					expectedGlobal: false,
					expectedLocal:  false,
				},
			},
		},
		{
			name:     "allLeaving",
			globalFn: globalChecker.allLeavingByIDs,
			localFn:  localChecker.allLeavingByIDs,
			cases: []placementCheckerTestCase{
				{
					instances:      []string{"i3", "i4"},
					expectedGlobal: true,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i3"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i4"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i1"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"i3,i6"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"non-existent"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{},
					expectedGlobal: false,
					expectedLocal:  false,
				},
			},
		},
		{
			name:     "allInitializing",
			globalFn: globalChecker.allInitializing,
			localFn:  localChecker.allInitializing,
			cases: []placementCheckerTestCase{
				{
					instances:      []string{"i5", "i6"},
					expectedGlobal: true,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i5"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i6"},
					expectedGlobal: false,
					expectedLocal:  true,
				},
				{
					instances:      []string{"i1"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"i5,i1"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{"non-existent"},
					expectedGlobal: false,
					expectedLocal:  false,
				},
				{
					instances:      []string{},
					expectedGlobal: false,
					expectedLocal:  false,
				},
			},
		},
	}

	for _, test := range tests {
		for _, tc := range test.cases {
			testName := fmt.Sprintf("%s(%+v)", test.name, tc.instances)
			//nolint:scopelint
			t.Run(testName, func(t *testing.T) {
				require.Equal(t, tc.expectedGlobal, test.globalFn(p, tc.instances, nowNanos))
				require.Equal(t, tc.expectedLocal, test.localFn(p, tc.instances, nowNanos))
			})
		}
	}
}

func newTestShards(s shard.State, minID, maxID uint32, cutoffNanos int64, cutoverNanos int64) shard.Shards {
	var shards []shard.Shard
	for id := minID; id <= maxID; id++ {
		s := shard.NewShard(id).SetState(s).SetCutoffNanos(cutoffNanos).SetCutoverNanos(cutoverNanos)
		shards = append(shards, s)
	}

	return shard.NewShards(shards)
}
