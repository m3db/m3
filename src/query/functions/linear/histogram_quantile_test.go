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

package linear

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatherSeriesToBuckets(t *testing.T) {
	name := []byte("name")
	bucket := []byte("bucket")
	tagOpts := models.NewTagOptions().
		SetIDSchemeType(models.TypeQuoted).
		SetMetricName(name).
		SetBucketName(bucket)

	tags := models.NewTags(3, tagOpts).SetName([]byte("foo")).AddTag(models.Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})

	noBucketMeta := block.SeriesMeta{Tags: tags}
	invalidBucketMeta := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("string"))}
	validMeta := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("0.1"))}
	validMeta2 := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("0.1"))}
	validMeta3 := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("10"))}
	infMeta := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("Inf"))}
	validMetaMoreTags := block.SeriesMeta{Tags: tags.Clone().SetBucket([]byte("0.1")).AddTag(models.Tag{
		Name:  []byte("qux"),
		Value: []byte("qar"),
	})}

	metas := []block.SeriesMeta{
		validMeta, noBucketMeta, invalidBucketMeta, validMeta2, validMetaMoreTags, validMeta3, infMeta,
	}

	actual := gatherSeriesToBuckets(metas)
	expected := bucketedSeries{
		`{bar="baz"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 0.1, idx: 0},
				{upperBound: 0.1, idx: 3},
				{upperBound: 10, idx: 5},
				{upperBound: math.Inf(1), idx: 6},
			},
			tags: models.NewTags(1, tagOpts).AddTag(models.Tag{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			}),
		},
		`{bar="baz",qux="qar"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 0.1, idx: 4},
			},
			tags: models.NewTags(1, tagOpts).AddTag(models.Tag{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			}).AddTag(models.Tag{
				Name:  []byte("qux"),
				Value: []byte("qar"),
			}),
		},
	}

	assert.Equal(t, sanitizeBuckets(expected), actual)
}

func TestSanitizeBuckets(t *testing.T) {
	bucketed := bucketedSeries{
		`{bar="baz"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 10, idx: 5},
				{upperBound: math.Inf(1), idx: 6},
				{upperBound: 1, idx: 0},
				{upperBound: 2, idx: 3},
			},
		},
		`{with="neginf"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 10, idx: 5},
				{upperBound: math.Inf(-1), idx: 6},
				{upperBound: 1, idx: 0},
				{upperBound: 2, idx: 3},
			},
		},
		`{no="infinity"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 0.1, idx: 4},
				{upperBound: 0.2, idx: 14},
				{upperBound: 0.3, idx: 114},
			},
		},
		`{just="infinity"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: math.Inf(1), idx: 4},
			},
		},
		`{just="neg-infinity"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: math.Inf(-1), idx: 4},
			},
		},
	}

	expected := validSeriesBuckets{
		indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 1, idx: 0},
				{upperBound: 2, idx: 3},
				{upperBound: 10, idx: 5},
				{upperBound: math.Inf(1), idx: 6},
			},
		},
	}

	assert.Equal(t, expected, sanitizeBuckets(bucketed))
}

func TestEnsureMonotonic(t *testing.T) {
	tests := []struct {
		name string
		data []bucketValue
		want []bucketValue
	}{
		{
			"empty",
			[]bucketValue{},
			[]bucketValue{},
		},
		{
			"one",
			[]bucketValue{{upperBound: 1, value: 5}},
			[]bucketValue{{upperBound: 1, value: 5}},
		},
		{
			"two monotonic",
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 6}},
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 6}},
		},
		{
			"two nonmonotonic",
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 4}},
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 5}},
		},
		{
			"three monotonic",
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 6}, {upperBound: 3, value: 7}},
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 6}, {upperBound: 3, value: 7}},
		},
		{
			"three nonmonotonic",
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 3}, {upperBound: 3, value: 4}},
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 5}, {upperBound: 3, value: 5}},
		},
		{
			"four nonmonotonic",
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 3}, {upperBound: 3, value: 6}, {upperBound: 4, value: 3}},
			[]bucketValue{{upperBound: 1, value: 5}, {upperBound: 2, value: 5}, {upperBound: 3, value: 6}, {upperBound: 4, value: 6}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ensureMonotonic(tt.data)
			assert.Equal(t, tt.want, tt.data)
		})
	}
}

func TestEnsureMonotonicPreserveNaN(t *testing.T) {
	data := []bucketValue{
		{upperBound: 1, value: 5},
		{upperBound: 2, value: 3},
		{upperBound: 3, value: math.NaN()},
		{upperBound: 4, value: 0},
	}
	ensureMonotonic(data)
	assert.Equal(t, data[0], bucketValue{upperBound: 1, value: 5})
	assert.Equal(t, data[1], bucketValue{upperBound: 2, value: 5})
	assert.Equal(t, data[2].upperBound, float64(3))
	assert.True(t, math.IsNaN(data[2].value))
	assert.Equal(t, data[3], bucketValue{upperBound: 4, value: 5})
}

func TestBucketQuantile(t *testing.T) {
	// single bucket returns nan
	actual := bucketQuantile(0.5, []bucketValue{{upperBound: 1, value: 1}})
	assert.True(t, math.IsNaN(actual))

	// bucket with no infinity returns nan
	actual = bucketQuantile(0.5, []bucketValue{
		{upperBound: 1, value: 1},
		{upperBound: 2, value: 2},
	})
	assert.True(t, math.IsNaN(actual))

	// bucket with negative infinity bound returns nan
	actual = bucketQuantile(0.5, []bucketValue{
		{upperBound: 1, value: 1},
		{upperBound: 2, value: 2},
		{upperBound: math.Inf(-1), value: 22},
	})
	assert.True(t, math.IsNaN(actual))

	actual = bucketQuantile(0.5, []bucketValue{
		{upperBound: 1, value: 1},
		{upperBound: math.Inf(1), value: 22},
	})
	assert.Equal(t, float64(1), actual)

	actual = bucketQuantile(0.8, []bucketValue{
		{upperBound: 2, value: 13},
		{upperBound: math.Inf(1), value: 71},
	})
	assert.Equal(t, float64(2), actual)

	// NB: tested against Prom
	buckets := []bucketValue{
		{upperBound: 1, value: 1},
		{upperBound: 2, value: 2},
		{upperBound: 5, value: 5},
		{upperBound: 10, value: 10},
		{upperBound: 20, value: 15},
		{upperBound: math.Inf(1), value: 16},
	}

	actual = bucketQuantile(0, buckets)
	assert.InDelta(t, float64(0), actual, 0.0001)

	actual = bucketQuantile(0.15, buckets)
	assert.InDelta(t, 2.4, actual, 0.0001)

	actual = bucketQuantile(0.2, buckets)
	assert.InDelta(t, float64(3.2), actual, 0.0001)

	actual = bucketQuantile(0.5, buckets)
	assert.InDelta(t, float64(8), actual, 0.0001)

	actual = bucketQuantile(0.8, buckets)
	assert.InDelta(t, float64(15.6), actual, 0.0001)

	actual = bucketQuantile(1, buckets)
	assert.InDelta(t, float64(20), actual, 0.0001)
}

func TestNewOp(t *testing.T) {
	args := make([]interface{}, 0, 1)
	_, err := NewHistogramQuantileOp(args, HistogramQuantileType)
	assert.Error(t, err)

	args = append(args, "invalid")
	_, err = NewHistogramQuantileOp(args, HistogramQuantileType)
	assert.Error(t, err)

	args[0] = 2.0
	_, err = NewHistogramQuantileOp(args, ClampMaxType)
	assert.Error(t, err)

	op, err := NewHistogramQuantileOp(args, HistogramQuantileType)
	assert.NoError(t, err)

	assert.Equal(t, HistogramQuantileType, op.OpType())
	assert.Equal(t, "type: histogram_quantile", op.String())
}

func testQuantileFunctionWithQ(t *testing.T, q float64) [][]float64 {
	args := make([]interface{}, 0, 1)
	args = append(args, q)
	op, err := NewHistogramQuantileOp(args, HistogramQuantileType)
	require.NoError(t, err)

	name := []byte("name")
	bucket := []byte("bucket")
	tagOpts := models.NewTagOptions().
		SetIDSchemeType(models.TypeQuoted).
		SetMetricName(name).
		SetBucketName(bucket)

	tags := models.NewTags(3, tagOpts).SetName([]byte("foo")).AddTag(models.Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})

	seriesMetas := []block.SeriesMeta{
		{Tags: tags.Clone().SetBucket([]byte("1"))},
		{Tags: tags.Clone().SetBucket([]byte("2"))},
		{Tags: tags.Clone().SetBucket([]byte("5"))},
		{Tags: tags.Clone().SetBucket([]byte("10"))},
		{Tags: tags.Clone().SetBucket([]byte("20"))},
		{Tags: tags.Clone().SetBucket([]byte("Inf"))},
		// this series should not be part of the output, since it has no bucket tag.
		{Tags: tags.Clone()},
	}

	v := [][]float64{
		{1, 1, 11, math.NaN(), math.NaN()},
		{2, 2, 12, 13, math.NaN()},
		{5, 5, 15, math.NaN(), math.NaN()},
		{10, 10, 20, math.NaN(), math.NaN()},
		{15, 15, 25, math.NaN(), math.NaN()},
		{16, 19, math.NaN(), 71, 1},
	}

	bounds := models.Bounds{
		Start:    xtime.Now(),
		Duration: time.Minute * 5,
		StepSize: time.Minute,
	}

	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(histogramQuantileOp).Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)

	return sink.Values
}

var (
	inf  = math.Inf(+1)
	ninf = math.Inf(-1)
)

func TestQuantileFunctionForInvalidQValues(t *testing.T) {
	actual := testQuantileFunctionWithQ(t, -1)
	assert.Equal(t, [][]float64{{ninf, ninf, ninf, ninf, ninf}}, actual)
	actual = testQuantileFunctionWithQ(t, 1.1)
	assert.Equal(t, [][]float64{{inf, inf, inf, inf, inf}}, actual)

	actual = testQuantileFunctionWithQ(t, 0.8)
	test.EqualsWithNansWithDelta(t, [][]float64{{15.6, 20, math.NaN(), 2, math.NaN()}}, actual, 0.00001)
}

func testWithMultipleBuckets(t *testing.T, q float64) [][]float64 {
	args := make([]interface{}, 0, 1)
	args = append(args, q)
	op, err := NewHistogramQuantileOp(args, HistogramQuantileType)
	require.NoError(t, err)

	name := []byte("name")
	bucket := []byte("bucket")
	tagOpts := models.NewTagOptions().
		SetIDSchemeType(models.TypeQuoted).
		SetMetricName(name).
		SetBucketName(bucket)

	tags := models.NewTags(3, tagOpts).SetName([]byte("foo")).AddTag(models.Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})

	tagsTwo := models.NewTags(3, tagOpts).SetName([]byte("qux")).AddTag(models.Tag{
		Name:  []byte("quaz"),
		Value: []byte("quail"),
	})

	seriesMetas := []block.SeriesMeta{
		{Tags: tags.Clone().SetBucket([]byte("1"))},
		{Tags: tags.Clone().SetBucket([]byte("2"))},
		{Tags: tags.Clone().SetBucket([]byte("5"))},
		{Tags: tags.Clone().SetBucket([]byte("10"))},
		{Tags: tags.Clone().SetBucket([]byte("20"))},
		{Tags: tags.Clone().SetBucket([]byte("Inf"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("1"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("2"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("5"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("10"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("20"))},
		{Tags: tagsTwo.Clone().SetBucket([]byte("Inf"))},
	}

	v := [][]float64{
		{1, 1, 11, math.NaN(), math.NaN()},
		{2, 2, 12, 13, math.NaN()},
		{5, 5, 15, math.NaN(), math.NaN()},
		{10, 10, 20, math.NaN(), math.NaN()},
		{15, 15, 25, math.NaN(), math.NaN()},
		{16, 19, math.NaN(), 71, 1},
		{21, 31, 411, math.NaN(), math.NaN()},
		{22, 32, 412, 513, math.NaN()},
		{25, 35, 415, math.NaN(), math.NaN()},
		{210, 310, 420, math.NaN(), math.NaN()},
		{215, 315, 425, math.NaN(), math.NaN()},
		{216, 319, math.NaN(), 571, 601},
	}

	bounds := models.Bounds{
		Start:    xtime.Now(),
		Duration: time.Minute * 5,
		StepSize: time.Minute,
	}

	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(histogramQuantileOp).Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)

	return sink.Values
}

func TestQuantileFunctionForMultipleBuckets(t *testing.T) {
	for i := 0; i < 100; i++ {
		actual := testWithMultipleBuckets(t, 0.8)
		expected := [][]float64{
			{15.6, 20, math.NaN(), 2, math.NaN()},
			{8.99459, 9.00363, math.NaN(), 1.78089, math.NaN()},
		}

		test.EqualsWithNansWithDelta(t, expected, actual, 0.00001)
	}
}
