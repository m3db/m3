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

package m3

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/sync"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	test1MonthRetention  = 30 * 24 * time.Hour
	test3MonthRetention  = 90 * 24 * time.Hour
	test6MonthRetention  = 180 * 24 * time.Hour
	test1YearRetention   = 365 * 24 * time.Hour
	testLongestRetention = test1YearRetention
)

type testSessions struct {
	unaggregated1MonthRetention                       *client.MockSession
	aggregated1MonthRetention1MinuteResolution        *client.MockSession
	aggregated3MonthRetention5MinuteResolution        *client.MockSession
	aggregatedPartial6MonthRetention1MinuteResolution *client.MockSession
	aggregated1YearRetention10MinuteResolution        *client.MockSession
}

func (s testSessions) forEach(fn func(session *client.MockSession)) {
	for _, session := range []*client.MockSession{
		s.unaggregated1MonthRetention,
		s.aggregated1MonthRetention1MinuteResolution,
		s.aggregated3MonthRetention5MinuteResolution,
		s.aggregatedPartial6MonthRetention1MinuteResolution,
		s.aggregated1YearRetention10MinuteResolution,
	} {
		fn(session)
	}
}

func setup(
	t *testing.T,
	ctrl *gomock.Controller,
) (storage.Storage, testSessions) {
	unaggregated1MonthRetention := client.NewMockSession(ctrl)
	aggregated1MonthRetention1MinuteResolution := client.NewMockSession(ctrl)
	aggregated3MonthRetention5MinuteResolution := client.NewMockSession(ctrl)
	aggregatedPartial6MonthRetention1MinuteResolution := client.NewMockSession(ctrl)
	aggregated1YearRetention10MinuteResolution := client.NewMockSession(ctrl)
	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unaggregated1MonthRetention,
		Retention:   test1MonthRetention,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_1m:30d"),
		Session:     aggregated1MonthRetention1MinuteResolution,
		Retention:   test1MonthRetention,
		Resolution:  time.Minute,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_5m:90d"),
		Session:     aggregated3MonthRetention5MinuteResolution,
		Retention:   test3MonthRetention,
		Resolution:  5 * time.Minute,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_partial_1m:180d"),
		Session:     aggregatedPartial6MonthRetention1MinuteResolution,
		Retention:   test6MonthRetention,
		Resolution:  1 * time.Minute,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_10m:365d"),
		Session:     aggregated1YearRetention10MinuteResolution,
		Retention:   test1YearRetention,
		Resolution:  10 * time.Minute,
	})
	require.NoError(t, err)
	return newTestStorage(t, clusters), testSessions{
		unaggregated1MonthRetention:                       unaggregated1MonthRetention,
		aggregated1MonthRetention1MinuteResolution:        aggregated1MonthRetention1MinuteResolution,
		aggregated3MonthRetention5MinuteResolution:        aggregated3MonthRetention5MinuteResolution,
		aggregatedPartial6MonthRetention1MinuteResolution: aggregatedPartial6MonthRetention1MinuteResolution,
		aggregated1YearRetention10MinuteResolution:        aggregated1YearRetention10MinuteResolution,
	}
}

func newTestStorage(t *testing.T, clusters Clusters) storage.Storage {
	writePool, err := sync.NewPooledWorkerPool(10, sync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	writePool.Init()
	opts := models.NewTagOptions().SetMetricName([]byte("name"))
	storage := NewStorage(opts, clusters, nil, writePool)
	return storage
}

func newFetchReq() *storage.FetchQuery {
	matchers := models.Matchers{
		{
			Type:  models.MatchEqual,
			Name:  []byte("foo"),
			Value: []byte("bar"),
		},
		{
			Type:  models.MatchEqual,
			Name:  []byte("biz"),
			Value: []byte("baz"),
		},
	}
	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       time.Now().Add(-10 * time.Minute),
		End:         time.Now(),
	}
}

func newWriteQuery() *storage.WriteQuery {
	tags := models.EmptyTags()
	tags.AddTags([]models.Tag{
		{Name: []byte("foo"), Value: []byte("bar")},
		{Name: []byte("biz"), Value: []byte("baz")},
	})

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
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTags := seriesiter.GenerateTag()

	session := sessions.unaggregated1MonthRetention
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTags, 1, 2), true, nil)
	session.EXPECT().IteratorPools().
		Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	searchReq := newFetchReq()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.NoError(t, err)
	tags := []models.Tag{{Name: testTags.Name.Bytes(), Value: testTags.Value.Bytes()}}
	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, 1)
	require.NotNil(t, results.SeriesList[0])
	assert.Equal(t, tags, results.SeriesList[0].Tags.Tags)
	assert.Equal(t, []byte("name"), results.SeriesList[0].Tags.Opts.GetMetricName())
}

func TestLocalReadExceedsRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTag := seriesiter.GenerateTag()

	session := sessions.aggregated1YearRetention10MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2), true, nil)
	session.EXPECT().IteratorPools().Return(nil, nil).AnyTimes()

	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * testLongestRetention)
	searchReq.End = time.Now()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func TestLocalReadExceedsUnaggregatedRetentionWithinAggregatedRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTag := seriesiter.GenerateTag()

	session := sessions.aggregated3MonthRetention5MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2), true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = sessions.aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators, true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching between 1month and 3 months (so 2 months) to hit multiple aggregated
	// namespaces that we need to choose from
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test1MonthRetention)
	searchReq.End = time.Now()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func TestLocalReadExceedsAggregatedButNotUnaggregatedAndPartialAggregated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	unaggregated1MonthRetention := client.NewMockSession(ctrl)
	aggregatedPartial6MonthRetention1MinuteResolution := client.NewMockSession(ctrl)

	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unaggregated1MonthRetention,
		Retention:   test1MonthRetention,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_1m:180d"),
		Session:     aggregatedPartial6MonthRetention1MinuteResolution,
		Retention:   test6MonthRetention,
		Resolution:  time.Minute,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
	})
	require.NoError(t, err)

	store := newTestStorage(t, clusters)

	testTag := seriesiter.GenerateTag()

	session := unaggregated1MonthRetention
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2), true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators, true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching past unaggregated namespace and verify that we fan out to both
	// the unaggregated namespaces and the partial aggregated namespace
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test1MonthRetention)
	searchReq.End = time.Now()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func TestLocalReadExceedsAggregatedAndPartialAggregated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	unaggregated1MonthRetention := client.NewMockSession(ctrl)
	aggregated3MonthRetention5MinuteResolution := client.NewMockSession(ctrl)
	aggregatedPartial6MonthRetention1MinuteResolution := client.NewMockSession(ctrl)

	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unaggregated1MonthRetention,
		Retention:   test1MonthRetention,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_5m:90d"),
		Session:     aggregated3MonthRetention5MinuteResolution,
		Retention:   test3MonthRetention,
		Resolution:  5 * time.Minute,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated_1m:180d"),
		Session:     aggregatedPartial6MonthRetention1MinuteResolution,
		Retention:   test6MonthRetention,
		Resolution:  time.Minute,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
	})
	require.NoError(t, err)

	store := newTestStorage(t, clusters)

	testTag := seriesiter.GenerateTag()

	session := aggregated3MonthRetention5MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2), true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators, true, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching past aggregated and partially aggregated namespace, fan out to both
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test6MonthRetention)
	searchReq.End = time.Now()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func assertFetchResult(t *testing.T, results *storage.FetchResult, testTag ident.Tag) {
	tags := []models.Tag{{
		Name:  testTag.Name.Bytes(),
		Value: testTag.Value.Bytes(),
	}}

	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, 1)
	require.NotNil(t, results.SeriesList[0])
	assert.Equal(t, tags, results.SeriesList[0].Tags.Tags)
}

func TestLocalSearchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	sessions.forEach(func(session *client.MockSession) {
		session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, false, fmt.Errorf("an error"))
		session.EXPECT().IteratorPools().
			Return(nil, nil).AnyTimes()
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
			namespace: "metrics_aggregated_1m:30d",
			tagName:   "qel",
			tagValue:  "quz",
		},
		{
			id:        "baz",
			namespace: "metrics_aggregated_5m:90d",
			tagName:   "qam",
			tagValue:  "qak",
		},
		{
			id:        "qux",
			namespace: "metrics_aggregated_10m:365d",
			tagName:   "qed",
			tagValue:  "qad",
		},
	}

	sessions.forEach(func(session *client.MockSession) {
		var f testFetchTaggedID
		switch {
		case session == sessions.unaggregated1MonthRetention:
			f = fetches[0]
		case session == sessions.aggregated1MonthRetention1MinuteResolution:
			f = fetches[1]
		case session == sessions.aggregated3MonthRetention5MinuteResolution:
			f = fetches[2]
		case session == sessions.aggregated1YearRetention10MinuteResolution:
			f = fetches[3]
		default:
			// Not expecting from other (partial) namespaces
			iter := client.NewMockTaggedIDsIterator(ctrl)
			gomock.InOrder(
				iter.EXPECT().Next().Return(false),
				iter.EXPECT().Err().Return(nil),
				iter.EXPECT().Finalize(),
			)
			session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(iter, true, nil)
			session.EXPECT().IteratorPools().
				Return(nil, nil).AnyTimes()
			return
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

		session.EXPECT().IteratorPools().
			Return(nil, nil).AnyTimes()
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
		assert.Equal(t, []models.Tag{{
			Name: []byte(expected.tagName), Value: []byte(expected.tagValue),
		}}, actual.Tags.Tags)
	}
}

func newTestIteratorPools(ctrl *gomock.Controller) encoding.IteratorPools {
	pools := encoding.NewMockIteratorPools(ctrl)

	mutablePool := encoding.NewMockMutableSeriesIteratorsPool(ctrl)
	mutablePool.EXPECT().
		Get(gomock.Any()).
		DoAndReturn(func(size int) encoding.MutableSeriesIterators {
			return encoding.NewSeriesIterators(make([]encoding.SeriesIterator, 0, size), mutablePool)
		}).
		AnyTimes()
	mutablePool.EXPECT().Put(gomock.Any()).AnyTimes()

	pools.EXPECT().MutableSeriesIterators().Return(mutablePool).AnyTimes()

	return pools
}
