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

package local

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/seriesiter"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRetention = 30 * 24 * time.Hour

type testSessions struct {
	unaggregated1MonthRetention                *client.MockSession
	aggregated1MonthRetention1MinuteResolution *client.MockSession
}

func (s testSessions) forEach(fn func(session *client.MockSession)) {
	for _, session := range []*client.MockSession{
		s.unaggregated1MonthRetention,
		s.aggregated1MonthRetention1MinuteResolution,
	} {
		fn(session)
	}
}

func setup(
	t *testing.T,
	ctrl *gomock.Controller,
) (storage.Storage, testSessions) {
	logging.InitWithCores(nil)
	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
	unaggregated1MonthRetention := client.NewMockSession(ctrl)
	aggregated1MonthRetention1MinuteResolution := client.NewMockSession(ctrl)
	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unaggregated1MonthRetention,
		Retention:   testRetention,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated"),
		Session:     aggregated1MonthRetention1MinuteResolution,
		Retention:   testRetention,
		Resolution:  time.Minute,
	})
	require.NoError(t, err)
	storage := NewStorage(clusters, nil)
	return storage, testSessions{
		unaggregated1MonthRetention:                unaggregated1MonthRetention,
		aggregated1MonthRetention1MinuteResolution: aggregated1MonthRetention1MinuteResolution,
	}
}

func newFetchReq() *storage.FetchQuery {
	matchers := models.Matchers{
		{
			Type:  models.MatchEqual,
			Name:  "foo",
			Value: "bar",
		},
		{
			Type:  models.MatchEqual,
			Name:  "biz",
			Value: "baz",
		},
	}
	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       time.Now().Add(-10 * time.Minute),
		End:         time.Now(),
	}
}

func newWriteQuery() *storage.WriteQuery {
	tags := map[string]string{"foo": "bar", "biz": "baz"}
	datapoints := ts.Datapoints{{
		Timestamp: time.Now(),
		Value:     1.0,
	},
		{
			Timestamp: time.Now().Add(-10 * time.Second),
			Value:     2.0,
		}}
	return &storage.WriteQuery{
		Tags:       tags,
		Unit:       xtime.Millisecond,
		Datapoints: datapoints,
	}
}

func setupLocalWrite(t *testing.T, ctrl *gomock.Controller) storage.Storage {
	store, sessions := setup(t, ctrl)
	session := sessions.unaggregated1MonthRetention
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	return store
}

func TestLocalWriteEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	err := store.Write(context.TODO(), nil)
	assert.Error(t, err)
}

func TestLocalWriteSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	writeQuery := newWriteQuery()
	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func TestLocalWriteAggregatedNoClusterNamespaceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)
	writeQuery := newWriteQuery()
	// Use unsupported retention/resolution
	writeQuery.Attributes = storage.Attributes{
		MetricsType: storage.AggregatedMetricsType,
		Retention:   1234,
		Resolution:  5678,
	}
	err := store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "no configured cluster namespace"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedInvalidMetricsTypeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)
	writeQuery := newWriteQuery()
	// Use unsupported retention/resolution
	writeQuery.Attributes = storage.Attributes{
		MetricsType: storage.MetricsType(math.MaxUint64),
		Retention:   30 * 24 * time.Hour,
	}
	err := store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "invalid write request"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)

	writeQuery := newWriteQuery()
	writeQuery.Attributes = storage.Attributes{
		MetricsType: storage.AggregatedMetricsType,
		Retention:   30 * 24 * time.Hour,
		Resolution:  time.Minute,
	}

	session := sessions.aggregated1MonthRetention1MinuteResolution
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(len(writeQuery.Datapoints))

	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func TestLocalRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTags := seriesiter.GenerateTag()
	sessions.forEach(func(session *client.MockSession) {
		session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(seriesiter.NewMockSeriesIters(ctrl, testTags, 1, 2), true, nil)
	})
	searchReq := newFetchReq()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.NoError(t, err)
	tags := make(models.Tags, 1)
	tags[testTags.Name.String()] = testTags.Value.String()
	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, 1)
	require.NotNil(t, results.SeriesList[0])
	assert.Equal(t, tags, results.SeriesList[0].Tags)
}

func TestLocalFetchBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, sessions := setup(t, ctrl)

	iterOne, err := test.BuildTestSeriesIterator()
	require.NoError(t, err)
	iterTwo, err := test.BuildTestSeriesIterator()
	require.NoError(t, err)
	iterators := encoding.NewSeriesIterators([]encoding.SeriesIterator{iterOne, iterTwo}, nil)

	sessions.forEach(func(session *client.MockSession) {
		session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).Return(iterators, true, nil)
	})
	searchReq := newFetchReq()
	results, err := store.FetchBlocks(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.NoError(t, err)
	require.NotNil(t, results)
	require.Len(t, results.Blocks, 2)

	stepIterOne, err := results.Blocks[0].StepIter()
	require.NoError(t, err)
	require.Equal(t, models.Tags{"foo": "bar", "baz": "qux"}, stepIterOne.Meta().Tags)

	stepIterTwo, err := results.Blocks[1].StepIter()
	require.NoError(t, err)
	require.Equal(t, models.Tags{"foo": "bar", "baz": "qux"}, stepIterTwo.Meta().Tags)
}

func TestLocalReadNoClustersForTimeRangeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * testRetention)
	searchReq.End = time.Now()
	_, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.Error(t, err)
	assert.Equal(t, errNoLocalClustersFulfillsQuery, err)
}

func TestLocalSearchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	sessions.forEach(func(session *client.MockSession) {
		session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, false, fmt.Errorf("an error"))
	})

	searchReq := newFetchReq()
	_, err := store.FetchTags(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.Error(t, err)
}

func TestLocalSearchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)

	type testFetchTaggedID struct {
		id        string
		namespace string
		tagName   string
		tagValue  string
	}

	fetches := []testFetchTaggedID{
		{
			id:        "foo",
			namespace: "metrics_unaggregated",
			tagName:   "qux",
			tagValue:  "qaz",
		},
		{
			id:        "bar",
			namespace: "metrics_aggregated",
			tagName:   "qel",
			tagValue:  "quz",
		},
	}

	sessions.forEach(func(session *client.MockSession) {
		var f testFetchTaggedID
		switch {
		case session == sessions.unaggregated1MonthRetention:
			f = fetches[0]
		case session == sessions.aggregated1MonthRetention1MinuteResolution:
			f = fetches[1]
		default:
			require.FailNow(t, "unexpected session")
		}
		iter := client.NewMockTaggedIDsIterator(ctrl)
		gomock.InOrder(
			iter.EXPECT().Next().Return(true),
			iter.EXPECT().Current().Return(
				ident.StringID(f.namespace),
				ident.StringID(f.id),
				ident.NewTagsIterator(ident.NewTags(
					ident.Tag{
						Name:  ident.StringID(f.tagName),
						Value: ident.StringID(f.tagValue),
					})),
			),
			iter.EXPECT().Next().Return(false),
			iter.EXPECT().Err().Return(nil),
			iter.EXPECT().Finalize(),
		)

		session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(iter, true, nil)
	})
	searchReq := newFetchReq()
	result, err := store.FetchTags(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.NoError(t, err)

	require.Equal(t, len(fetches), len(result.Metrics))

	expected := make(map[string]testFetchTaggedID)
	for _, f := range fetches {
		expected[f.id] = f
	}

	actual := make(map[string]*models.Metric)
	for _, m := range result.Metrics {
		actual[m.ID] = m
	}

	for id, actual := range actual {
		expected, ok := expected[id]
		require.True(t, ok)

		assert.Equal(t, expected.id, actual.ID)
		assert.Equal(t, expected.namespace, actual.Namespace)
		assert.Equal(t, models.Tags{
			expected.tagName: expected.tagValue,
		}, actual.Tags)
	}
}
