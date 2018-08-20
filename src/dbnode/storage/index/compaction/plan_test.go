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
	opts := DefaultOptions
	s1 := Segment{
		Age:  (opts.MutableCompactionAge - time.Second),
		Size: 10,
		Type: segments.MutableType,
	}
	plan, err := NewPlan([]Segment{s1}, opts)
	require.NoError(t, err)
	require.Equal(t, &Plan{UnusedSegments: []Segment{s1}}, plan)
}

func TestSingleMutableCompaction(t *testing.T) {
	opts := DefaultOptions
	candidates := []Segment{
		Segment{
			Age:  (opts.MutableCompactionAge + time.Second),
			Size: 10,
			Type: segments.MutableType,
		},
	}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	require.Equal(t, &Plan{
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
			Age:  (opts.MutableCompactionAge - time.Second),
			Size: 10,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Age:  (opts.MutableCompactionAge + time.Second),
			Size: 10,
			Type: segments.MutableType,
		}
	)
	candidates := []Segment{s1, s2}
	plan, err := NewPlan(candidates, opts)
	require.NoError(t, err)
	require.Equal(t, &Plan{
		UnusedSegments: []Segment{s1},
		Tasks: []Task{
			Task{Segments: []Segment{s2}},
		},
		OrderBy: opts.OrderBy,
	}, plan)
}

func TestMarkUnusedSegmentsSingleTier(t *testing.T) {
	opts := testOptions()
	var (
		s1 = Segment{
			Age:  (opts.MutableCompactionAge + time.Second),
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
			Age:  (opts.MutableCompactionAge + time.Second),
			Size: maxBucketSize + 1,
			Type: segments.MutableType,
		}
		s2 = Segment{
			Size: maxBucketSize + 1,
			Type: segments.FSTType,
		}
		s3 = Segment{
			Age:  (opts.MutableCompactionAge + time.Second),
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
