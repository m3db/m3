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
	"testing"

	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
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
	assert.False(t, merge.Exhaustive)
	assert.False(t, merge.LocalOnly)
	assert.Equal(t, 2, len(merge.Warnings))

	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())

	// ensure warnings are deduplicated
	merge = merge.CombineMetadata(rTwo)
	assert.Equal(t, 2, len(merge.Warnings))

	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())

	merge.AddWarning("foo", "bar")
	assert.Equal(t, 2, len(merge.Warnings))

	assert.Equal(t, "foo_bar", merge.Warnings[0].Header())
	assert.Equal(t, "baz_qux", merge.Warnings[1].Header())
}
