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

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/test/seriesiter"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3x/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyExpandSeries(ctx context.Context, t *testing.T, ctrl *gomock.Controller, num int) {
	testTags := seriesiter.GenerateTag()
	iters := seriesiter.NewMockSeriesIters(ctrl, testTags, num)

	results, err := SeriesIteratorsToFetchResult(ctx, iters, ident.StringID("strID"))
	assert.NoError(t, err)

	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, num)
	expectedTags := make(models.Tags, 1)
	expectedTags[testTags.Name.String()] = testTags.Value.String()

	for i := 0; i < num; i++ {
		series := results.SeriesList[i]
		require.NotNil(t, series)
		assert.Equal(t, expectedTags, series.Tags)
		fmt.Println(series.Values())
	}
}

func TestExpandSeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	logging.InitWithCores(nil)
	ctx := context.TODO()
	logger := logging.WithContext(ctx)
	defer logger.Sync()

	for i := 0; i < 100; i++ {
		verifyExpandSeries(ctx, t, ctrl, i)
	}
}
