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

package consolidators

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExhaustiveTagMerge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	r := NewMultiFetchTagsResult(models.NewTagOptions())
	for _, tt := range exhaustTests {
		t.Run(tt.name, func(t *testing.T) {
			for _, ex := range tt.exhaustives {
				it := client.NewMockTaggedIDsIterator(ctrl)
				it.EXPECT().Next().Return(false)
				it.EXPECT().Err().Return(nil)
				it.EXPECT().Finalize().Return()
				meta := block.NewResultMetadata()
				meta.Exhaustive = ex
				r.Add(it, meta, nil)
			}

			tagResult, err := r.FinalResult()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, tagResult.Metadata.Exhaustive)
			assert.NoError(t, r.Close())
		})
	}
}

func TestMultiFetchTagsResult(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	iter := client.NewMockTaggedIDsIterator(ctrl)
	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return(
		ident.StringID("ns"),
		ident.StringID("id"),
		ident.MustNewTagStringsIterator("foo", "bar"))
	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return(
		ident.StringID("ns"),
		ident.StringID("id"),
		ident.MustNewTagStringsIterator("foo", "baz"))
	iter.EXPECT().Next().Return(false)
	iter.EXPECT().Err()

	opts := models.NewTagOptions().SetFilters(models.Filters{
		models.Filter{Name: b("foo"), Values: [][]byte{b("baz")}},
	})

	r := NewMultiFetchTagsResult(opts)
	r.Add(iter, block.NewResultMetadata(), nil)

	res, err := r.FinalResult()
	require.NoError(t, err)

	require.Equal(t, 1, len(res.Tags))
	assert.Equal(t, "id", res.Tags[0].ID.String())
	it := res.Tags[0].Iter

	// NB: assert tags are still iteratable.
	ex := []tag{{name: "foo", value: "bar"}}
	for i := 0; it.Next(); i++ {
		tag := it.Current()
		assert.Equal(t, ex[i].name, tag.Name.String())
		assert.Equal(t, ex[i].value, tag.Value.String())
	}

	require.Equal(t, 1, res.Metadata.FetchedSeriesCount)
	require.NoError(t, it.Err())
}
