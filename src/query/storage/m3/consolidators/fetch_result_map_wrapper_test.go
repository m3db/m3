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
	"fmt"
	"testing"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchResultMapWrapper(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	size := 4
	fetchMap := newFetchResultMapWrapper(size)
	assert.Equal(t, 0, fetchMap.len())
	assert.Equal(t, 0, len(fetchMap.list()))
	_, found := fetchMap.get(models.EmptyTags())
	assert.False(t, found)

	tagName := "tag"
	tags := func(i int) models.Tags {
		return models.MustMakeTags(tagName, fmt.Sprint(i))
	}

	series := func(i int) encoding.SeriesIterator {
		it := encoding.NewMockSeriesIterator(ctrl)
		it.EXPECT().ID().Return(ident.StringID(fmt.Sprint(i))).AnyTimes()
		return it
	}

	for i := 0; i < size*2; i++ {
		fetchMap.set(tags(i), multiResultSeries{iter: series(i)})
	}

	assert.Equal(t, 8, fetchMap.len())
	assert.Equal(t, 8, len(fetchMap.list()))
	for i, l := range fetchMap.list() {
		ex := fmt.Sprint(i)
		assert.Equal(t, ex, l.iter.ID().String())
		v, found := l.tags.Get([]byte(tagName))
		require.True(t, found)
		assert.Equal(t, fmt.Sprint(i), string(v))
	}

	// Overwrite tag 7.
	fetchMap.set(tags(7), multiResultSeries{iter: series(700)})

	assert.Equal(t, 8, fetchMap.len())
	assert.Equal(t, 8, len(fetchMap.list()))
	for i, l := range fetchMap.list() {
		ex := fmt.Sprint(i)
		if i == 7 {
			assert.Equal(t, "700", l.iter.ID().String())
		} else {
			assert.Equal(t, ex, l.iter.ID().String())
		}

		v, found := l.tags.Get([]byte(tagName))
		require.True(t, found)
		assert.Equal(t, fmt.Sprint(i), string(v))
	}
}
