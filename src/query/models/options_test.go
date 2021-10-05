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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultTagOptions(t *testing.T) {
	opts := NewTagOptions()
	assert.NoError(t, opts.Validate())
	assert.Equal(t, defaultMetricName, opts.MetricName())
	assert.Equal(t, TypeQuoted, opts.IDSchemeType())
}

func TestValidTagOptions(t *testing.T) {
	opts := NewTagOptions().
		SetIDSchemeType(TypePrependMeta).
		SetMetricName([]byte("name")).
		SetBucketName([]byte("bucket"))

	assert.NoError(t, opts.Validate())
	assert.Equal(t, []byte("name"), opts.MetricName())
	assert.Equal(t, []byte("bucket"), opts.BucketName())
	assert.Equal(t, TypePrependMeta, opts.IDSchemeType())
}

func TestBadNameTagOptions(t *testing.T) {
	msg := errNoName.Error()
	opts := NewTagOptions().
		SetMetricName(nil)
	assert.EqualError(t, opts.Validate(), msg)

	opts = NewTagOptions().
		SetMetricName([]byte{})
	assert.EqualError(t, opts.Validate(), msg)
}

func TestBadBucketTagOptions(t *testing.T) {
	msg := errNoBucket.Error()
	opts := NewTagOptions().
		SetBucketName(nil)
	assert.EqualError(t, opts.Validate(), msg)

	opts = NewTagOptions().
		SetBucketName([]byte{})
	assert.EqualError(t, opts.Validate(), msg)
}

func TestBadSchemeTagOptions(t *testing.T) {
	msg := "invalid config id schema type 'unknown': should be one of" +
		" [quoted prepend_meta graphite]"
	opts := NewTagOptions().
		SetIDSchemeType(IDSchemeType(6))
	assert.EqualError(t, opts.Validate(), msg)
}

func TestOptionsEquals(t *testing.T) {
	opts, other := NewTagOptions(), NewTagOptions()
	assert.True(t, opts.Equals(other))

	bad := []byte("aaa")
	n := opts.BucketName()
	opts = opts.SetBucketName(bad)
	assert.False(t, opts.Equals(other))

	opts = opts.SetBucketName(n)
	n = opts.MetricName()
	opts = opts.SetMetricName(bad)
	assert.False(t, opts.Equals(other))

	opts = opts.SetMetricName(n)
	opts = opts.SetIDSchemeType(IDSchemeType(10))
	assert.False(t, opts.Equals(other))

	opts = NewTagOptions().SetMaxTagLiteralLength(42)
	assert.False(t, opts.Equals(other))
}
