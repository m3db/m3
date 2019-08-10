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
	"fmt"
	"testing"

	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MustMakeTags creates tags given that the number of args is even.
func MustMakeTags(tag ...string) models.Tags {
	if len(tag)%2 != 0 {
		panic("must have even tag length")
	}

	tagLength := len(tag) / 2
	t := models.NewTags(tagLength, models.NewTagOptions())
	for i := 0; i < tagLength; i++ {
		t = t.AddTag(models.Tag{
			Name:  []byte(tag[i*2]),
			Value: []byte(tag[i*2+1]),
		})
	}

	return t
}

// MustMakeMeta creates metadata with given bounds and tags provided the number
// is even.
func MustMakeMeta(bounds models.Bounds, tags ...string) Metadata {
	return Metadata{
		Tags:   MustMakeTags(tags...),
		Bounds: bounds,
	}
}

// MustMakeSeriesMeta creates series metadata with given bounds and tags
// provided the number is even.
func MustMakeSeriesMeta(tags ...string) SeriesMeta {
	return SeriesMeta{
		Tags: MustMakeTags(tags...),
	}
}

func CompareMeta(t *testing.T, ex, ac Metadata) {
	expectedTags := ex.Tags.Tags
	actualTags := ac.Tags.Tags
	require.Equal(t, len(expectedTags), len(actualTags))
	for i, tag := range expectedTags {
		fmt.Println("x", string(tag.Name), ":", string(tag.Value))
		fmt.Println("a", string(actualTags[i].Name), ":", string(actualTags[i].Value))
		assert.Equal(t, string(tag.Name), string(actualTags[i].Name))
		assert.Equal(t, string(tag.Value), string(actualTags[i].Value))
	}

	assert.True(t, ex.Bounds.Equals(ac.Bounds))
}
