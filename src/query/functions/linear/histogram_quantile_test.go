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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, expected, actual)
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

	actual := bucketedSeries{
		`{bar="baz"}`: indexedBuckets{
			buckets: []indexedBucket{
				{upperBound: 1, idx: 0},
				{upperBound: 2, idx: 3},
				{upperBound: 10, idx: 5},
				{upperBound: math.Inf(1), idx: 6},
			},
		},
	}

	sanitizeBuckets(bucketed)
	assert.Equal(t, actual, bucketed)
}
