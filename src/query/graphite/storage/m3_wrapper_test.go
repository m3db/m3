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

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	m3ts "github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTralsateQuery(t *testing.T) {
	query := `foo.ba[rz].q*x.terminator.will.be.back?`
	end := time.Now()
	start := end.Add(time.Hour * -2)
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	translated := translateQuery(query, opts)
	assert.Equal(t, end, translated.End)
	assert.Equal(t, start, translated.Start)
	assert.Equal(t, time.Duration(0), translated.Interval)
	assert.Equal(t, query, translated.Raw)
	matchers := translated.TagMatchers
	expected := models.Matchers{
		{Type: models.MatchRegexp, Name: []byte("__graphite0__"), Value: []byte("foo")},
		{Type: models.MatchRegexp, Name: []byte("__graphite1__"), Value: []byte("ba[rz]")},
		{Type: models.MatchRegexp, Name: []byte("__graphite2__"), Value: []byte("q.*x")},
		{Type: models.MatchRegexp, Name: []byte("__graphite3__"), Value: []byte("terminator")},
		{Type: models.MatchRegexp, Name: []byte("__graphite4__"), Value: []byte("will")},
		{Type: models.MatchRegexp, Name: []byte("__graphite5__"), Value: []byte("be")},
		{Type: models.MatchRegexp, Name: []byte("__graphite6__"), Value: []byte("back?")},
		{Type: models.MatchNotRegexp, Name: []byte("__graphite7__"), Value: []byte(".*")},
	}

	assert.Equal(t, expected, matchers)
}

func TestTranslateTimeseries(t *testing.T) {
	ctx := xctx.New()
	start := time.Now()
	expected := 5
	seriesList := make(m3ts.SeriesList, expected)
	for i := 0; i < expected; i++ {
		vals := m3ts.NewFixedStepValues(10*time.Second, 1, float64(i), start)
		seriesList[i] = m3ts.NewSeries(fmt.Sprint("a", i), vals, models.NewTags(0, nil))
	}

	translated := translateTimeseries(ctx, seriesList, start)
	require.Equal(t, expected, len(translated))
	for i, tt := range translated {
		ex := []float64{float64(i)}
		assert.Equal(t, ex, tt.SafeValues())
		assert.Equal(t, fmt.Sprint("a", i), tt.Name())
	}
}

func TestFetchByQuery(t *testing.T) {
	store := mock.NewMockStorage()
	start := time.Now().Add(time.Hour * -1)
	vals := m3ts.NewFixedStepValues(10*time.Second, 3, 3, start)
	seriesList := m3ts.SeriesList{
		m3ts.NewSeries("a", vals, models.NewTags(0, nil)),
	}

	store.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	wrapper := NewM3WrappedStorage(store)
	ctx := xctx.New()
	ctx.SetRequestContext(context.TODO())
	end := time.Now()
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	query := "a*b"
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	assert.NoError(t, err)
	require.Equal(t, 1, len(result.SeriesList))
	series := result.SeriesList[0]
	assert.Equal(t, "a", series.Name())
	assert.Equal(t, []float64{3, 3, 3}, series.SafeValues())
}
