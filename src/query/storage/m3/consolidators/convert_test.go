// Copyright (c) 2020 Uber Technologies, Inc.
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

package consolidators

import (
	"testing"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromIdentTagIteratorToTags(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	tagFn := func(n, v string) ident.Tag {
		return ident.Tag{
			Name:  ident.StringID(n),
			Value: ident.StringID(v),
		}
	}

	it := ident.NewMockTagIterator(ctrl)
	it.EXPECT().Remaining().Return(2)
	it.EXPECT().Next().Return(true)
	it.EXPECT().Current().Return(tagFn("foo", "bar"))
	it.EXPECT().Next().Return(true)
	it.EXPECT().Current().Return(tagFn("baz", "qux"))
	it.EXPECT().Next().Return(false)
	it.EXPECT().Err().Return(nil)

	opts := models.NewTagOptions().SetIDSchemeType(models.TypeQuoted)
	tags, err := FromIdentTagIteratorToTags(it, opts)
	require.NoError(t, err)
	require.Equal(t, 2, tags.Len())
	assert.Equal(t, `{baz="qux",foo="bar"}`, string(tags.ID()))
	assert.Equal(t, []byte("__name__"), tags.Opts.MetricName())
}
