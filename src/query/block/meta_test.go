// Copyright (c) 2019 Uber Technologies, Inc.
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

package block

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeta(t *testing.T) {
	bounds, otherBounds := models.Bounds{}, models.Bounds{}
	badBounds := models.Bounds{Duration: 100}

	assert.True(t, bounds.Equals(otherBounds))
	assert.False(t, bounds.Equals(badBounds))
}

func TestResultMeta(t *testing.T) {
	r := NewResultMetadata()
	assert.True(t, r.Exhaustive)
	assert.True(t, r.LocalOnly)
	assert.Equal(t, 0, len(r.Warnings))
	assert.True(t, r.IsDefault())

	r.AddWarning("foo", "bar")
	assert.Equal(t, 1, len(r.Warnings))
	assert.Equal(t, "foo", r.Warnings[0].Name)
	assert.Equal(t, "bar", r.Warnings[0].Message)
	assert.False(t, r.IsDefault())

	rTwo := ResultMetadata{
		LocalOnly:  false,
		Exhaustive: false,
	}

	assert.False(t, rTwo.IsDefault())
	rTwo.AddWarning("baz", "qux")
	merge := r.CombineMetadata(rTwo)
	assert.Equal(t, 1, len(r.Warnings))
	assert.Equal(t, 1, len(rTwo.Warnings))

	assert.False(t, merge.Exhaustive)
	assert.False(t, merge.LocalOnly)
	require.Equal(t, 2, len(merge.Warnings))
	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())

	// ensure warnings are deduplicated
	merge = merge.CombineMetadata(rTwo)
	require.Equal(t, 2, len(merge.Warnings))

	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())

	merge.AddWarning("foo", "bar")
	require.Equal(t, 2, len(merge.Warnings))

	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())
}

func TestMergeEmptyWarnings(t *testing.T) {
	r := NewResultMetadata()
	r.AddWarning("foo", "bar")
	rTwo := NewResultMetadata()
	merge := r.CombineMetadata(rTwo)
	assert.Equal(t, 1, len(r.Warnings))
	assert.Equal(t, 0, len(rTwo.Warnings))
	require.Equal(t, 1, len(merge.Warnings))
	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
}

func TestMergeResultMetricMetadata(t *testing.T) {
	r1 := ResultMetricMetadata{
		WithSamples: 100,
		NoSamples:   20,
		Aggregated:  10,
	}
	r2 := ResultMetricMetadata{
		WithSamples:  1,
		Aggregated:   2,
		Unaggregated: 3,
	}
	r1.Merge(r2)
	assert.Equal(t, 101, r1.WithSamples)
	assert.Equal(t, 20, r1.NoSamples)
	assert.Equal(t, 12, r1.Aggregated)
	assert.Equal(t, 3, r1.Unaggregated)
}

func TestCombineMetadataWithMetricMetadataMaps(t *testing.T) {
	r1 := NewResultMetadata()
	r2 := NewResultMetadata()
	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	noname := []byte{}

	r1.ByName(a).WithSamples = 5
	r1.ByName(c).Aggregated = 1
	r1.ByName(noname).Unaggregated = 19

	r2.ByName(nil).Aggregated = 99
	r2.ByName(a).WithSamples = 6
	r2.ByName(a).NoSamples = 1
	r2.ByName(b).NoSamples = 1
	r2.ByName(b).Unaggregated = 15
	r2.ByName(c).Aggregated = 2

	r := r1.CombineMetadata(r2)
	assert.Equal(t, &ResultMetricMetadata{
		NoSamples:   1,
		WithSamples: 11,
	}, r.ByName(a))
	assert.Equal(t, &ResultMetricMetadata{
		NoSamples:    1,
		Unaggregated: 15,
	}, r.ByName(b))
	assert.Equal(t, &ResultMetricMetadata{
		Aggregated: 3,
	}, r.ByName(c))
	assert.Equal(t, &ResultMetricMetadata{
		Unaggregated: 19,
		Aggregated:   99,
	}, r.ByName(nil))

	// Sanity check that accessing by nil or by an empty name
	// yields the same result.
	assert.Equal(t, r.ByName(nil), r.ByName(noname))

	// And finally it should marshal to JSON without error.
	js, err := json.Marshal(r)
	assert.Nil(t, err)
	assert.NotEmpty(t, string(js))
}

func TestMergeIntoEmptyWarnings(t *testing.T) {
	r := NewResultMetadata()
	rTwo := NewResultMetadata()
	rTwo.AddWarning("foo", "bar")
	merge := r.CombineMetadata(rTwo)
	assert.Equal(t, 0, len(r.Warnings))
	assert.Equal(t, 1, len(rTwo.Warnings))
	require.Equal(t, 1, len(merge.Warnings))
	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
}

func TestMergeResolutions(t *testing.T) {
	expected := []time.Duration{1, 2, 3}
	r := ResultMetadata{}
	rTwo := ResultMetadata{}
	merge := r.CombineMetadata(rTwo)
	assert.Nil(t, r.Resolutions)
	assert.Nil(t, rTwo.Resolutions)
	assert.Nil(t, merge.Resolutions)

	rTwo.Resolutions = expected
	merge = r.CombineMetadata(rTwo)
	assert.Nil(t, r.Resolutions)
	assert.Equal(t, 3, len(rTwo.Resolutions))
	require.Equal(t, 3, len(merge.Resolutions))
	assert.Equal(t, expected, merge.Resolutions)

	r = ResultMetadata{Resolutions: expected}
	rTwo = ResultMetadata{}
	merge = r.CombineMetadata(rTwo)
	assert.Equal(t, 3, len(r.Resolutions))
	assert.Nil(t, rTwo.Resolutions)
	require.Equal(t, 3, len(merge.Resolutions))
	assert.Equal(t, expected, merge.Resolutions)

	rTwo = ResultMetadata{Resolutions: []time.Duration{4, 5, 6}}
	merge = r.CombineMetadata(rTwo)
	assert.Equal(t, 3, len(r.Resolutions))
	assert.Equal(t, 3, len(rTwo.Resolutions))
	require.Equal(t, 6, len(merge.Resolutions))
	assert.Equal(t, []time.Duration{1, 2, 3, 4, 5, 6}, merge.Resolutions)
}

func TestVerifyTemporalRange(t *testing.T) {
	r := ResultMetadata{
		Exhaustive:  true,
		Resolutions: []time.Duration{5, 10},
	}

	ex0 := "resolution larger than query range_range: 1ns, resolutions: 10ns, 5ns"
	ex1 := "resolution larger than query range_range: 6ns, resolutions: 10ns"

	r.VerifyTemporalRange(11)
	assert.Equal(t, 0, len(r.WarningStrings()))

	r.VerifyTemporalRange(1)
	require.Equal(t, 1, len(r.WarningStrings()))
	assert.Equal(t, ex0, r.WarningStrings()[0])

	r.VerifyTemporalRange(6)
	require.Equal(t, 2, len(r.WarningStrings()))
	assert.Equal(t, ex0, r.WarningStrings()[0])
	assert.Equal(t, ex1, r.WarningStrings()[1])

	r.VerifyTemporalRange(11)
	require.Equal(t, 2, len(r.WarningStrings()))
	assert.Equal(t, ex0, r.WarningStrings()[0])
	assert.Equal(t, ex1, r.WarningStrings()[1])
}
