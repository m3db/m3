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
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/sync"
	bytetest "github.com/m3db/m3/src/x/test"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
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
	writePool, err := sync.NewPooledWorkerPool(10,
		sync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	writePool.Init()
	opts := models.NewTagOptions().SetMetricName([]byte("name"))
	storage, err := NewStorage(clusters, nil, writePool, opts, time.Minute,
		instrument.NewOptions())
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

type writeQueryOptions struct {
	attributes *storage.Attributes
}

func newWriteQuery(opts writeQueryOptions) storage.WriteQuery {
	attrs := storage.Attributes{
		MetricsType: storage.UnaggregatedMetricsType,
	}
	if v := opts.attributes; v != nil {
		attrs = *v
	}

	writeQuery := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: ident.NewTagsIterator(ident.NewTags(
			ident.Tag{Name: ident.StringID("foo"), Value: ident.StringID("bar")},
			ident.Tag{Name: ident.StringID("biz"), Value: ident.StringID("baz")},
		)),
		TagOptions: models.NewTagOptions().
			SetIDSchemeType(models.TypeQuoted),
		Datapoints: ts.Datapoints{
			ts.Datapoint{
				Timestamp: time.Now().Add(-10 * time.Second),
				Value:     2.0,
			},
		},
		Unit:       xtime.Millisecond,
		Attributes: attrs,
	})
	return writeQuery
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
	err := store.Write(context.TODO(), storage.WriteQuery{})
	assert.Error(t, err)
}

func TestLocalWriteSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := setupLocalWrite(t, ctrl)
	writeQuery := newWriteQuery(writeQueryOptions{})
	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
	assert.NoError(t, store.Close())
}

func TestLocalWriteAggregatedNoClusterNamespaceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)
	writeQuery := newWriteQuery(writeQueryOptions{
		// Use unsupported retention/resolution
		attributes: &storage.Attributes{
			MetricsType: storage.AggregatedMetricsType,
			Retention:   1234,
			Resolution:  5678,
		},
	})
	err := store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "no configured cluster namespace"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedInvalidMetricsTypeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, _ := setup(t, ctrl)
	writeQuery := newWriteQuery(writeQueryOptions{
		// Use unsupported retention/resolution
		attributes: &storage.Attributes{
			MetricsType: storage.MetricsType(math.MaxUint64),
			Retention:   30 * 24 * time.Hour,
		},
	})
	err := store.Write(context.TODO(), writeQuery)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "invalid write request"),
		fmt.Sprintf("unexpected error string: %v", err.Error()))
}

func TestLocalWriteAggregatedSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)

	writeQuery := newWriteQuery(writeQueryOptions{
		attributes: &storage.Attributes{
			MetricsType: storage.AggregatedMetricsType,
			Retention:   30 * 24 * time.Hour,
			Resolution:  time.Minute,
		},
	})

	session := sessions.aggregated1MonthRetention1MinuteResolution
	session.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(len(writeQuery.Datapoints()))

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
	results, err := store.Fetch(context.TODO(), searchReq, buildFetchOpts())
	assert.NoError(t, err)
	tags := []models.Tag{{Name: testTags.Name.Bytes(), Value: testTags.Value.Bytes()}}
	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, 1)
	require.NotNil(t, results.SeriesList[0])
	assert.Equal(t, tags, results.SeriesList[0].Tags.Tags)
	assert.Equal(t, []byte("name"), results.SeriesList[0].Tags.Opts.MetricName())
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
	results, err := store.Fetch(context.TODO(), searchReq, buildFetchOpts())
	require.NoError(t, err)
	assertFetchResult(t, results, testTag)
}

func buildFetchOpts() *storage.FetchOptions {
	opts := storage.NewFetchOptions()
	opts.Limit = 100
	return opts
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
	results, err := store.Fetch(context.TODO(), searchReq, buildFetchOpts())
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
	results, err := store.Fetch(context.TODO(), searchReq, buildFetchOpts())
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
	results, err := store.Fetch(context.TODO(), searchReq, buildFetchOpts())
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
	_, err := store.SearchSeries(context.TODO(), searchReq, buildFetchOpts())
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
	ctrl := gomock.NewController(t)
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
				Return(iter, true, nil)
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
			Return(iter, true, nil)
	})

	req := newCompleteTagsReq()
	result, err := store.CompleteTags(context.TODO(), req, buildFetchOpts())
	require.NoError(t, err)

	require.False(t, result.CompleteNameOnly)
	require.Equal(t, 3, len(result.CompletedTags))
	// NB: expected will be sorted alphabetically
	expected := []storage.CompletedTag{
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
	ctrl := gomock.NewController(t)
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
		Return(iter, true, nil)

	req := newCompleteTagsReq()
	result, err := store.CompleteTags(context.TODO(), req, buildFetchOpts())
	require.NoError(t, err)

	require.False(t, result.CompleteNameOnly)
	require.Equal(t, 1, len(result.CompletedTags))
	// NB: expected will be sorted alphabetically
	expected := []storage.CompletedTag{
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

func TestLocalWriteBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	unagg := client.NewMockSession(ctrl)
	nsID := ident.StringID("metrics_unaggregated")
	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: nsID,
		Session:     unagg,
		Retention:   test1MonthRetention,
	})
	require.NoError(t, err)

	store := newTestStorage(t, clusters)

	now := time.Now().Truncate(time.Millisecond)
	nowMillis := now.UnixNano() / int64(time.Millisecond)

	input := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels: []*prompb.Label{
					&prompb.Label{Name: b("foo"), Value: b("bar")},
					&prompb.Label{Name: b("bar"), Value: b("baz")},
				},
				Samples: []*prompb.Sample{
					&prompb.Sample{
						Value:     42.0,
						Timestamp: nowMillis,
					},
					&prompb.Sample{
						Value:     84.0,
						Timestamp: nowMillis,
					},
				},
			},
			&prompb.TimeSeries{
				Labels: []*prompb.Label{
					&prompb.Label{Name: b("qux"), Value: b("qar")},
					&prompb.Label{Name: b("qar"), Value: b("qaz")},
				},
				Samples: []*prompb.Sample{
					&prompb.Sample{
						Value:     123.0,
						Timestamp: nowMillis,
					},
				},
			},
		},
	}

	numDatapoints := 0
	for _, s := range input.Timeseries {
		numDatapoints += len(s.Samples)
	}

	// Marshal.
	data, err := proto.Marshal(input)
	require.NoError(t, err)

	// Snappy encode.
	encoded := snappy.Encode(nil, data)

	httpReq := httptest.NewRequest("POST", "/write", bytes.NewReader(encoded))
	httpReq.ContentLength = int64(len(encoded))

	req, err := remote.NewParser().ParseWriteRequest(httpReq)
	require.NoError(t, err)

	defer req.Finalize()

	tagOpts := models.NewTagOptions().
		SetIDSchemeType(models.TypeQuoted)

	seriesIter := remote.NewTimeSeriesIter(req, tagOpts)

	attr := storage.Attributes{
		MetricsType: storage.UnaggregatedMetricsType,
	}

	writeIter := ingest.NewWriteQueryIter(seriesIter)
	writeIter.Reset(attr)

	unagg.EXPECT().
		WriteTaggedBatch(gomock.Any()).
		DoAndReturn(func(iter client.WriteTaggedIter) error {
			// Simulate the client iteration.
			for restarts := 0; restarts < 2; restarts++ {
				if restarts > 0 {
					// Shouldn't need to restart the first time.
					iter.Restart()
				}

				i := 0
				for ; iter.Next(); i++ {
					state := iter.State()

					if restarts == 0 {
						assert.Nil(t, state)
						iter.SetState(i)
					} else {
						assert.Equal(t, i, state.(int))
					}

					curr := iter.Current()

					switch i {
					case 0:
						assert.Equal(t, nsID.String(), curr.Namespace.String())
						assert.Equal(t, `{bar="baz",foo="bar"}`, curr.ID.String())
						assert.True(t, ident.NewTagIterMatcher(
							ident.MustNewTagStringsIterator(
								"bar", "baz", "foo", "bar")).Matches(curr.Tags))
						assert.True(t, now.Equal(curr.Timestamp),
							fmt.Sprintf("expected=%s, actual=%s",
								now.String(),
								curr.Timestamp.String()))
						assert.Equal(t, 42.0, curr.Value)
						assert.Equal(t, xtime.Millisecond, curr.Unit)
						assert.Nil(t, curr.Annotation)
					case 1:
						assert.Equal(t, nsID.String(), curr.Namespace.String())
						assert.Equal(t, `{bar="baz",foo="bar"}`, curr.ID.String())
						assert.True(t, ident.NewTagIterMatcher(
							ident.MustNewTagStringsIterator(
								"bar", "baz", "foo", "bar")).Matches(curr.Tags))
						assert.True(t, now.Equal(curr.Timestamp),
							fmt.Sprintf("expected=%s, actual=%s",
								now.String(),
								curr.Timestamp.String()))
						assert.Equal(t, 84.0, curr.Value)
						assert.Equal(t, xtime.Millisecond, curr.Unit)
						assert.Nil(t, curr.Annotation)
					case 2:
						assert.Equal(t, nsID.String(), curr.Namespace.String())
						assert.Equal(t, `{qar="qaz",qux="qar"}`, curr.ID.String())
						assert.True(t, ident.NewTagIterMatcher(
							ident.MustNewTagStringsIterator(
								"qar", "qaz", "qux", "qar")).Matches(curr.Tags))
						assert.True(t, now.Equal(curr.Timestamp),
							fmt.Sprintf("expected=%s, actual=%s",
								now.String(),
								curr.Timestamp.String()))
						assert.Equal(t, 123.0, curr.Value)
						assert.Equal(t, xtime.Millisecond, curr.Unit)
						assert.Nil(t, curr.Annotation)
					}
				}

				// Check iter error.
				require.NoError(t, iter.Err())

				// Should iterate through all samples.
				require.Equal(t, numDatapoints, i)
			}

			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = store.WriteBatch(ctx, writeIter)
	require.NoError(t, err)
}
