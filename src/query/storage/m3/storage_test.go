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
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/sync"
	bytetest "github.com/m3db/m3/src/x/test"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

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

var (
	testFetchResponseMetadata = client.FetchResponseMetadata{Exhaustive: true}
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
	writePool, err := sync.NewPooledWorkerPool(10,
		sync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	writePool.Init()
	tagOpts := models.NewTagOptions().SetMetricName([]byte("name"))
	opts := m3db.NewOptions().
		SetWriteWorkerPool(writePool).
		SetLookbackDuration(time.Minute).
		SetTagOptions(tagOpts)
	storage, err := NewStorage(clusters, opts, instrument.NewTestOptions(t))
	require.NoError(t, err)
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

func newWriteQuery(t *testing.T) *storage.WriteQuery {
	tags := models.EmptyTags().AddTags([]models.Tag{
		{Name: []byte("foo"), Value: []byte("bar")},
		{Name: []byte("biz"), Value: []byte("baz")},
	})

	q, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: tags,
		Unit: xtime.Millisecond,
		Datapoints: ts.Datapoints{
			{
				Timestamp: time.Now(),
				Value:     1.0,
			},
			{
				Timestamp: time.Now().Add(-10 * time.Second),
				Value:     2.0,
			},
		},
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
	require.NoError(t, err)

	return q
}

func setupLocalWrite(t *testing.T, ctrl *gomock.Controller) storage.Storage {
	store, sessions := setup(t, ctrl)
	session := sessions.unaggregated1MonthRetention
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	return store
}

func TestLocalWriteEmpty(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	err := store.Write(context.TODO(), nil)
	assert.Error(t, err)
}

func TestLocalWriteSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	writeQuery := newWriteQuery(t)
	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func TestLocalWriteAggregatedNoClusterNamespaceError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)

	opts := newWriteQuery(t).Options()

	// Use unsupported retention/resolution
	opts.Attributes = storagemetadata.Attributes{
		MetricsType: storagemetadata.AggregatedMetricsType,
		Retention:   1234,
		Resolution:  5678,
	}

	writeQuery, err := storage.NewWriteQuery(opts)
	require.NoError(t, err)

	err = store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "no configured cluster namespace"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteUnaggregatedNamespaceUninitializedError(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	// We setup an empty dynamic cluster, which will by default
	// have an uninitialized unaggregated namespace.
	store := newTestStorage(t, &dynamicCluster{})

	opts := newWriteQuery(t).Options()

	writeQuery, err := storage.NewWriteQuery(opts)
	require.NoError(t, err)

	err = store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unaggregated namespace is not yet initialized"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedInvalidMetricsTypeError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)

	opts := newWriteQuery(t).Options()

	// Use unsupported retention/resolution
	opts.Attributes = storagemetadata.Attributes{
		MetricsType: storagemetadata.MetricsType(math.MaxUint64),
		Retention:   30 * 24 * time.Hour,
	}

	writeQuery, err := storage.NewWriteQuery(opts)
	require.NoError(t, err)

	err = store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "invalid write request"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)

	opts := newWriteQuery(t).Options()

	// Use unsupported retention/resolution
	opts.Attributes = storagemetadata.Attributes{
		MetricsType: storagemetadata.AggregatedMetricsType,
		Retention:   30 * 24 * time.Hour,
		Resolution:  time.Minute,
	}

	writeQuery, err := storage.NewWriteQuery(opts)
	require.NoError(t, err)

	session := sessions.aggregated1MonthRetention1MinuteResolution
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(len(writeQuery.Datapoints()))

	err = store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func TestLocalRead(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store, sessions := setup(t, ctrl)
	testTags := seriesiter.GenerateTag()

	session := sessions.unaggregated1MonthRetention
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTags, 1, 2),
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().
		Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	searchReq := newFetchReq()
	results, err := store.FetchProm(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTags)
}

func TestLocalReadExceedsRetention(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTag := seriesiter.GenerateTag()

	session := sessions.aggregated1YearRetention10MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2),
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().
		Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * testLongestRetention)
	searchReq.End = time.Now()
	results, err := store.FetchProm(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

// TestLocalWriteWithExpiredContext ensures that writes are at least attempted
// even with an expired context, this is so that data is not lost even if
// the original writer has already disconnected.
func TestLocalWriteWithExpiredContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	writeQuery := newWriteQuery(t)

	past := time.Now().Add(-time.Minute)

	ctx, cancel := context.WithDeadline(context.Background(), past)
	defer cancel()

	// Ensure expired.
	var expired bool
	select {
	case <-ctx.Done():
		expired = true
	default:
	}
	require.True(t, expired, "context expected to be expired")

	err := store.Write(ctx, writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

// TestLocalWritesWithExpiredContext ensures that writes are at least attempted
// even with an expired context, this is so that data is not lost even if
// the original writer has already disconnected.
func TestLocalWritesWithExpiredContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	writeQueryOpts := newWriteQuery(t).Options()
	writeQueryOpts.Datapoints = ts.Datapoints{
		ts.Datapoint{
			Timestamp: time.Now(),
			Value:     42,
		},
		ts.Datapoint{
			Timestamp: time.Now(),
			Value:     84,
		},
	}
	writeQuery, err := storage.NewWriteQuery(writeQueryOpts)
	require.NoError(t, err)

	past := time.Now().Add(-time.Minute)

	ctx, cancel := context.WithDeadline(context.Background(), past)
	defer cancel()

	// Ensure expired.
	var expired bool
	select {
	case <-ctx.Done():
		expired = true
	default:
	}
	require.True(t, expired, "context expected to be expired")

	err = store.Write(ctx, writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func buildFetchOpts() *storage.FetchOptions {
	opts := storage.NewFetchOptions()
	opts.SeriesLimit = 100
	return opts
}

func TestLocalReadExceedsUnaggregatedRetentionWithinAggregatedRetention(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	testTag := seriesiter.GenerateTag()

	session := sessions.aggregated3MonthRetention5MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2),
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = sessions.aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators,
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching between 1month and 3 months (so 2 months) to hit multiple aggregated
	// namespaces that we need to choose from
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test1MonthRetention)
	searchReq.End = time.Now()
	results, err := store.FetchProm(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func TestLocalReadExceedsAggregatedButNotUnaggregatedAndPartialAggregated(t *testing.T) {
	ctrl := xtest.NewController(t)
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
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2),
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators,
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching past unaggregated namespace and verify that we fan out to both
	// the unaggregated namespaces and the partial aggregated namespace
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test1MonthRetention)
	searchReq.End = time.Now()
	results, err := store.FetchProm(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func TestLocalReadExceedsAggregatedAndPartialAggregated(t *testing.T) {
	ctrl := xtest.NewController(t)
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
		Return(seriesiter.NewMockSeriesIters(ctrl, testTag, 1, 2),
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	session = aggregatedPartial6MonthRetention1MinuteResolution
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(encoding.EmptySeriesIterators,
			testFetchResponseMetadata, nil)
	session.EXPECT().IteratorPools().Return(newTestIteratorPools(ctrl), nil).AnyTimes()

	// Test searching past aggregated and partially aggregated namespace, fan out to both
	searchReq := newFetchReq()
	searchReq.Start = time.Now().Add(-2 * test6MonthRetention)
	searchReq.End = time.Now()
	results, err := store.FetchProm(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func assertFetchResult(t *testing.T, results storage.PromResult, testTag ident.Tag) {
	require.NotNil(t, results.PromResult)
	series := results.PromResult.GetTimeseries()
	require.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	require.Equal(t, 1, len(labels))
	l := labels[0]
	assert.Equal(t, testTag.Name.String(), string(l.GetName()))
	assert.Equal(t, testTag.Value.String(), string(l.GetValue()))
}

func TestLocalSearchError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	sessions.forEach(func(session *client.MockSession) {
		session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, client.FetchResponseMetadata{Exhaustive: false}, fmt.Errorf("an error"))
		session.EXPECT().IteratorPools().
			Return(nil, nil).AnyTimes()
	})

	searchReq := newFetchReq()
	_, err := store.SearchSeries(context.TODO(), searchReq, buildFetchOpts())
	assert.Error(t, err)
}

func TestLocalSearchSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
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
				Return(iter, testFetchResponseMetadata, nil)
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
			Return(iter, testFetchResponseMetadata, nil)

		session.EXPECT().IteratorPools().
			Return(nil, nil).AnyTimes()
	})
	searchReq := newFetchReq()
	result, err := store.SearchSeries(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)

	require.Equal(t, len(fetches), len(result.Metrics))

	expected := make(map[string]testFetchTaggedID)
	for _, f := range fetches {
		expected[f.id] = f
	}

	actual := make(map[string]models.Metric)
	for _, m := range result.Metrics {
		actual[string(m.ID)] = m
	}

	for id, actual := range actual {
		expected, ok := expected[id]
		require.True(t, ok)

		assert.Equal(t, []byte(expected.id), actual.ID)
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

func newCompleteTagsReq() *storage.CompleteTagsQuery {
	matchers := models.Matchers{
		{
			Type:  models.MatchEqual,
			Name:  []byte("qux"),
			Value: []byte(".*"),
		},
	}

	return &storage.CompleteTagsQuery{
		CompleteNameOnly: false,
		FilterNameTags:   [][]byte{[]byte("qux")},
		TagMatchers:      matchers,
	}
}

func TestLocalCompleteTagsSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)

	type testFetchTaggedID struct {
		tagName  string
		tagValue string
	}

	fetches := []testFetchTaggedID{
		{
			tagName:  "qux",
			tagValue: "qaz",
		},
		{
			tagName:  "aba",
			tagValue: "quz",
		},
		{
			tagName:  "qam",
			tagValue: "qak",
		},
		{
			tagName:  "qux",
			tagValue: "qaz2",
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
			iter := client.NewMockAggregatedTagsIterator(ctrl)
			gomock.InOrder(
				iter.EXPECT().Remaining().Return(0),
				iter.EXPECT().Next().Return(false),
				iter.EXPECT().Err().Return(nil),
				iter.EXPECT().Finalize(),
			)
			session.EXPECT().Aggregate(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(iter, testFetchResponseMetadata, nil)
			return
		}

		iter := client.NewMockAggregatedTagsIterator(ctrl)
		gomock.InOrder(
			iter.EXPECT().Remaining().Return(1),
			iter.EXPECT().Next().Return(true),
			iter.EXPECT().Current().Return(
				ident.StringID(f.tagName),
				ident.NewIDsIterator(ident.StringID(f.tagValue)),
			),
			iter.EXPECT().Next().Return(false),
			iter.EXPECT().Err().Return(nil),
			iter.EXPECT().Finalize(),
		)

		session.EXPECT().Aggregate(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(iter, testFetchResponseMetadata, nil)
	})

	req := newCompleteTagsReq()
	result, err := store.CompleteTags(context.TODO(), req, buildFetchOpts())
	require.NoError(t, err)

	require.False(t, result.CompleteNameOnly)
	require.Equal(t, 3, len(result.CompletedTags))
	// NB: expected will be sorted alphabetically
	expected := []consolidators.CompletedTag{
		{
			Name:   []byte("aba"),
			Values: [][]byte{[]byte("quz")},
		},
		{
			Name:   []byte("qam"),
			Values: [][]byte{[]byte("qak")},
		},
		{
			Name:   []byte("qux"),
			Values: [][]byte{[]byte("qaz"), []byte("qaz2")},
		},
	}

	assert.Equal(t, expected, result.CompletedTags)
}

func TestLocalCompleteTagsSuccessFinalize(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	unagg := client.NewMockSession(ctrl)
	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unagg,
		Retention:   test1MonthRetention,
	})

	require.NoError(t, err)
	store := newTestStorage(t, clusters)

	name, value := ident.StringID("name"), ident.StringID("value")
	iter := client.NewMockAggregatedTagsIterator(ctrl)
	gomock.InOrder(
		iter.EXPECT().Remaining().Return(1),
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return(
			name,
			ident.NewIDsIterator(value),
		),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(nil),
		iter.EXPECT().Finalize().Do(func() {
			name.Finalize()
			value.Finalize()
		}),
	)

	unagg.EXPECT().Aggregate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(iter, testFetchResponseMetadata, nil)

	req := newCompleteTagsReq()
	result, err := store.CompleteTags(context.TODO(), req, buildFetchOpts())
	require.NoError(t, err)

	require.False(t, result.CompleteNameOnly)
	require.Equal(t, 1, len(result.CompletedTags))
	// NB: expected will be sorted alphabetically
	expected := []consolidators.CompletedTag{
		{
			Name:   []byte("name"),
			Values: [][]byte{[]byte("value")},
		},
	}

	require.Equal(t, expected, result.CompletedTags)

	// ensure that the tag names and values are not backed by the same data.
	n, v := result.CompletedTags[0].Name, result.CompletedTags[0].Values[0]
	assert.False(t, bytetest.ByteSlicesBackedBySameData(name.Bytes(), n))
	assert.False(t, bytetest.ByteSlicesBackedBySameData(value.Bytes(), v))
}

func TestInvalidBlockTypes(t *testing.T) {
	opts := m3db.NewOptions()
	s, err := NewStorage(nil, opts, instrument.NewOptions())
	require.NoError(t, err)

	query := &storage.FetchQuery{}
	fetchOpts := &storage.FetchOptions{BlockType: models.TypeMultiBlock}
	_, err = s.FetchBlocks(context.TODO(), query, fetchOpts)
	assert.Error(t, err)
}
