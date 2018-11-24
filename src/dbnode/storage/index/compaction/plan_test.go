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

package compaction

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/index/segments"

	"github.com/stretchr/testify/require"
)

func TestDefaultOptsValidate(t *testing.T) {
	require.NoError(t, DefaultOptions.Validate())
}
func TestBrandNewMutableSegment(t *testing.T) {
	opts := PlannerOptions{
		MutableCompactionAgeThreshold: 10 * time.Second,
		MutableSegmentSizeThreshold:   10,
	}
	seg := Segment{
		Age:  (opts.MutableCompactionAgeThreshold - time.Second),
		Size: opts.MutableSegmentSizeThreshold - 1,
		Type: segments.MutableType,
	}
	require.False(t, seg.Compactable(opts))
}

func TestJustOldEnoughMutableSegment(t *testing.T) {
	opts := PlannerOptions{
		MutableCompactionAgeThreshold: 10 * time.Second,
		MutableSegmentSizeThreshold:   10,
	}
	seg := Segment{
		Age:  (opts.MutableCompactionAgeThreshold + time.Second),
		Size: opts.MutableSegmentSizeThreshold - 1,
		Type: segments.MutableType,
	}
	require.True(t, seg.Compactable(opts))
}

func TestJustLargeEnoughMutableSegment(t *testing.T) {
	opts := PlannerOptions{
		MutableCompactionAgeThreshold: 10 * time.Second,
		MutableSegmentSizeThreshold:   10,
	}
	seg := Segment{
		Age:  (opts.MutableCompactionAgeThreshold - time.Second),
		Size: opts.MutableSegmentSizeThreshold + 1,
		Type: segments.MutableType,
	}
	require.True(t, seg.Compactable(opts))
}

func TestSingleMutableCompaction(t *testing.T) {
	opts := DefaultOptions
	candidates := []Segment{
		Segment{
			Age:  (opts.MutableCompactionAgeThreshold + time.Second),
			Size: 10,
			Type: segments.MutableType,
		},
	}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	requirePlansEqual(t, &Plan{
		Tasks: []Task{
			Task{
				Segments: candidates,
			},
		},
		OrderBy: opts.OrderBy,
	}, plan)
}

func TestReportNewMutableSegmentUnused(t *testing.T) {
	opts := DefaultOptions
	var (
		s1 = Segment{
			Age:  (opts.MutableCompactionAgeThreshold - time.Second),
			Size: 10,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Age:  (opts.MutableCompactionAgeThreshold + time.Second),
			Size: 10,
			Type: segments.MutableType,
		}
	)
	candidates := []Segment{s1, s2}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	requirePlansEqual(t, &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s1, s2}},
		},
		OrderBy: opts.OrderBy,
	}, plan)
}

func TestMarkUnusedSegmentsSingleTier(t *testing.T) {
	opts := testOptions()
	var (
		s1 = Segment{
			Age:  (opts.MutableCompactionAgeThreshold + time.Second),
			Size: 10,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Size: 60,
			Type: segments.FSTType,
		}
		s3 = Segment{
			Size: 61,
			Type: segments.FSTType,
		}
	)
	candidates := []Segment{s1, s3, s2}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	require.Equal(t, &Plan{
		UnusedSegments: []Segment{s3},
		Tasks: []Task{
			Task{Segments: []Segment{s1, s2}},
		},
		OrderBy: opts.OrderBy,
	}, plan)
}

func TestDontCompactSegmentTooLarge(t *testing.T) {
	opts := testOptions()
	sort.Sort(ByMinSize(opts.Levels))
	maxBucketSize := opts.Levels[len(opts.Levels)-1].MaxSizeExclusive
	var (
		s1 = Segment{
			Age:  (opts.MutableCompactionAgeThreshold + time.Second),
			Size: maxBucketSize + 1,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Size: maxBucketSize + 1,
			Type: segments.FSTType,
		}
		s3 = Segment{
			Age:  (opts.MutableCompactionAgeThreshold + time.Second),
			Size: 61,
			Type: segments.MutableType,
		}
		s4 = Segment{
			Size: 128,
			Type: segments.FSTType,
		}
	)
	candidates := []Segment{s1, s2, s3, s4}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	require.Equal(t, &Plan{
		UnusedSegments: []Segment{s2, s4}, // s2 is too large to be compacted, s4 is by itself in the second tier
		Tasks: []Task{
			Task{Segments: []Segment{s3}}, // s3 is small enough to be compacted
			Task{Segments: []Segment{s1}}, // s1 should be compacted regardless of size because its mutable
		},
		OrderBy: opts.OrderBy,
	}, plan)
}

func TestPlanOrderByMutableAge(t *testing.T) {
	var (
		s1 = Segment{
			Age:  10,
			Size: 1,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Age:  10,
			Size: 10,
			Type: segments.MutableType,
		}
		s3 = Segment{
			Age:  100,
			Size: 10,
			Type: segments.MutableType,
		}
	)
	p := &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s1, s2}},
			Task{Segments: []Segment{s3}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}
	sort.Sort(p)
	requirePlansEqual(t, &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s3}},
			Task{Segments: []Segment{s1, s2}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}, p)
}
func TestPlanOrderByNumMutable(t *testing.T) {
	var (
		s1 = Segment{
			Age:  5,
			Size: 1,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Age:  5,
			Size: 10,
			Type: segments.MutableType,
		}
		s3 = Segment{
			Age:  10,
			Size: 10,
			Type: segments.MutableType,
		}
	)
	p := &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s3}},
			Task{Segments: []Segment{s1, s2}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}
	sort.Sort(p)
	requirePlansEqual(t, &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s1, s2}},
			Task{Segments: []Segment{s3}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}, p)
}

func TestPlanOrderByMutableCumulativeSize(t *testing.T) {
	var (
		s1 = Segment{
			Age:  10,
			Size: 10,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Age:  10,
			Size: 2,
			Type: segments.MutableType,
		}
		s3 = Segment{
			Age:  15,
			Size: 10,
			Type: segments.MutableType,
		}
		s4 = Segment{
			Age:  5,
			Size: 6,
			Type: segments.MutableType,
		}
	)
	p := &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s3, s4}},
			Task{Segments: []Segment{s1, s2}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}
	sort.Sort(p)
	requirePlansEqual(t, &Plan{
		Tasks: []Task{
			Task{Segments: []Segment{s1, s2}},
			Task{Segments: []Segment{s3, s4}},
		},
		OrderBy: TasksOrderedByOldestMutableAndSize,
	}, p)
}

func requirePlansEqual(t *testing.T, expected, observed *Plan) {
	if expected == nil {
		require.Nil(t, observed)
		return
	}
	require.Equal(t, len(expected.UnusedSegments), len(observed.UnusedSegments),
		fmt.Sprintf("exp [%+v]\nobs[%+v]", expected.UnusedSegments, observed.UnusedSegments))
	for i := range expected.UnusedSegments {
		require.Equal(t, expected.UnusedSegments[i], observed.UnusedSegments[i], i)
	}
	require.Equal(t, len(expected.Tasks), len(observed.Tasks),
		fmt.Sprintf("exp [%+v]\nobs[%+v]", expected.Tasks, observed.Tasks))
	for i := range expected.Tasks {
		require.Equal(t, expected.Tasks[i], observed.Tasks[i])
	}
	require.Equal(t, expected.OrderBy, observed.OrderBy)
}

func testOptions() PlannerOptions {
	opts := DefaultOptions
	opts.Levels = []Level{ // i.e. tiers for compaction [0, 64), [64, 524), [524, 4000)
		Level{
			MinSizeInclusive: 0,
			MaxSizeExclusive: 64,
		},
		Level{
			MinSizeInclusive: 64,
			MaxSizeExclusive: 524,
		},
		Level{
			MinSizeInclusive: 524,
			MaxSizeExclusive: 4000,
		},
	}
	return opts
}
