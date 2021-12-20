//
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

package fanout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	errs "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/policy/filter"
	"github.com/m3db/m3/src/query/storage"
	storagem3 "github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func filterFunc(output bool) filter.Storage {
	return func(query storage.Query, store storage.Storage) bool {
		return output
	}
}

func filterCompleteTagsFunc(output bool) filter.StorageCompleteTags {
	return func(query storage.CompleteTagsQuery, store storage.Storage) bool {
		return output
	}
}

func fakeIterator(t *testing.T) encoding.SeriesIterators {
	id := ident.StringID("id")
	namespace := ident.StringID("metrics")
	return encoding.NewSeriesIterators([]encoding.SeriesIterator{
		encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
			ID:        id,
			Namespace: namespace,
			Tags: seriesiter.GenerateSingleSampleTagIterator(
				xtest.NewController(t), seriesiter.GenerateTag()),
		}, nil),
	}, nil)
}

type fetchResponse struct {
	result encoding.SeriesIterators
	err    error
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

func setupFanoutRead(t *testing.T, output bool, response ...*fetchResponse) storage.Storage {
	if len(response) == 0 {
		response = []*fetchResponse{{err: fmt.Errorf("unable to get response")}}
	}

	ctrl := xtest.NewController(t)
	store1, session1 := m3.NewStorageAndSession(t, ctrl)
	store2, session2 := m3.NewStorageAndSession(t, ctrl)
	pools := newTestIteratorPools(ctrl)
	session1.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(response[0].result, client.FetchResponseMetadata{Exhaustive: true}, response[0].err)
	session2.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(response[len(response)-1].result, client.FetchResponseMetadata{Exhaustive: true}, response[len(response)-1].err)
	session1.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, errs.ErrNotImplemented)
	session2.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, errs.ErrNotImplemented)
	session1.EXPECT().IteratorPools().
		Return(pools, nil).AnyTimes()
	session2.EXPECT().IteratorPools().
		Return(pools, nil).AnyTimes()

	stores := []storage.Storage{
		store1, store2,
	}

	store := NewStorage(stores, filterFunc(output), filterFunc(output),
		filterCompleteTagsFunc(output), models.NewTagOptions(),
		storagem3.NewOptions(encoding.NewOptions()), instrument.NewOptions())
	return store
}

func setupFanoutWrite(t *testing.T, output bool, errs ...error) storage.Storage {
	ctrl := xtest.NewController(t)
	store1, session1 := m3.NewStorageAndSession(t, ctrl)
	store2, session2 := m3.NewStorageAndSession(t, ctrl)
	session1.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(errs[0])
	session1.EXPECT().IteratorPools().
		Return(nil, nil).AnyTimes()
	session1.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: true}, errs[0]).AnyTimes()
	session1.EXPECT().Aggregate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: true}, errs[0]).AnyTimes()

	session2.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(errs[len(errs)-1])
	session2.EXPECT().IteratorPools().
		Return(nil, nil).AnyTimes()

	stores := []storage.Storage{
		store1, store2,
	}
	store := NewStorage(stores, filterFunc(output), filterFunc(output),
		filterCompleteTagsFunc(output), models.NewTagOptions(),
		storagem3.NewOptions(encoding.NewOptions()), instrument.NewOptions())
	return store
}

func TestCompleteTags_RestrictOptionsWorks_SingleStore(t *testing.T) {
	ctrl := xtest.NewController(t)
	store := storage.NewMockStorage(ctrl)

	meta := block.NewResultMetadata()
	meta.Exhaustive = false
	fullResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: []byte("bar"), Values: xtest.BytesArray("zulu", "quail")},
			{Name: []byte("foo"), Values: xtest.BytesArray("quail")},
		},

		Metadata: meta,
	}

	store.EXPECT().CompleteTags(gomock.Any(), gomock.Any(), gomock.Any()).Return(fullResult, nil)

	stores := []storage.Storage{store}
	fanoutStorage := NewStorage(stores, filterFunc(false), filterFunc(false),
		filterCompleteTagsFunc(true), models.NewTagOptions(),
		storagem3.NewOptions(encoding.NewOptions()), instrument.NewOptions())

	fetchOptions := storage.NewFetchOptions()
	fetchOptions.RestrictQueryOptions = &storage.RestrictQueryOptions{
		RestrictByTag: &storage.RestrictByTag{
			Strip: xtest.BytesArray("bar"),
		},
	}

	completeTagsResult, err := fanoutStorage.CompleteTags(
		context.TODO(),
		&storage.CompleteTagsQuery{
			CompleteNameOnly: true,
			TagMatchers:      models.Matchers{},
		},
		fetchOptions)

	require.NoError(t, err)

	actualTags := completeTagsResult.CompletedTags
	require.Len(t, actualTags, 1)
	require.Equal(t, []byte("foo"), actualTags[0].Name)
}

func TestQueryStorageMetadataAttributes(t *testing.T) {
	ctrl := xtest.NewController(t)
	store1, _ := m3.NewStorageAndSession(t, ctrl)

	stores := []storage.Storage{store1}

	store := NewStorage(stores, filterFunc(false), filterFunc(false),
		filterCompleteTagsFunc(false), models.NewTagOptions(),
		storagem3.NewOptions(encoding.NewOptions()), instrument.NewOptions())

	attrs, err := store.QueryStorageMetadataAttributes(
		context.Background(),
		time.Now().Add(-10*time.Minute),
		time.Now(),
		storage.NewFetchOptions(),
	)
	require.NoError(t, err)
	require.Equal(t, []storagemetadata.Attributes{
		{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   m3.TestRetention,
		},
	}, attrs)
}

func TestQueryStorageMetadataAttributesMultipleStores(t *testing.T) {
	ctrl := xtest.NewController(t)
	aggNs1 := []storagem3.AggregatedClusterNamespaceDefinition{
		{
			NamespaceID: ident.StringID("5m:90d"),
			Resolution:  5 * time.Minute,
			Retention:   120 * 24 * time.Hour,
		},
	}
	aggNs2 := []storagem3.AggregatedClusterNamespaceDefinition{
		{
			NamespaceID: ident.StringID("5m:90d"),
			Resolution:  5 * time.Minute,
			Retention:   120 * 24 * time.Hour,
		},
		{
			NamespaceID: ident.StringID("10m:120d"),
			Resolution:  10 * time.Minute,
			Retention:   150 * 24 * time.Hour,
		},
	}
	store1, _ := m3.NewStorageAndSessionWithAggregatedNamespaces(t, ctrl, aggNs1)
	store2, _ := m3.NewStorageAndSessionWithAggregatedNamespaces(t, ctrl, aggNs2)

	stores := []storage.Storage{store1, store2}

	store := NewStorage(stores, filterFunc(false), filterFunc(false),
		filterCompleteTagsFunc(false), models.NewTagOptions(),
		storagem3.NewOptions(encoding.NewOptions()), instrument.NewOptions())

	fetchOpts := storage.NewFetchOptions()
	fetchOpts.FanoutOptions.FanoutAggregated = storage.FanoutForceEnable
	attrs, err := store.QueryStorageMetadataAttributes(
		context.Background(),
		time.Now().Add(-200*24*time.Hour),
		time.Now(),
		fetchOpts,
	)
	require.NoError(t, err)

	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Retention < attrs[j].Retention
	})
	require.Equal(t, []storagemetadata.Attributes{
		{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Resolution:  5 * time.Minute,
			Retention:   120 * 24 * time.Hour,
		},
		{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Resolution:  10 * time.Minute,
			Retention:   150 * 24 * time.Hour,
		},
	}, attrs)
}

func TestFanoutReadEmpty(t *testing.T) {
	store := setupFanoutRead(t, false)
	res, err := store.FetchProm(context.TODO(), nil, storage.NewFetchOptions())
	assert.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 0, len(res.PromResult.GetTimeseries()))
	assert.Equal(t, 0, res.Metadata.FetchedSeriesCount)
	assert.Equal(t, block.ResultMetricMetadata{}, res.Metadata.MetadataByNameMerged())
}

func TestFanoutReadError(t *testing.T) {
	store := setupFanoutRead(t, true)
	opts := storage.NewFetchOptions()
	_, err := store.FetchProm(context.TODO(), &storage.FetchQuery{}, opts)
	assert.Error(t, err)
}

func TestFanoutReadSuccess(t *testing.T) {
	store := setupFanoutRead(t, true, &fetchResponse{
		result: fakeIterator(t),
	},
		&fetchResponse{result: fakeIterator(t)},
	)
	res, err := store.FetchProm(context.TODO(), &storage.FetchQuery{
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
	}, storage.NewFetchOptions())
	require.NoError(t, err, "no error on read")
	assert.NotNil(t, res)
	assert.NoError(t, store.Close())
}

func TestFanoutSearchEmpty(t *testing.T) {
	store := setupFanoutRead(t, false)
	res, err := store.SearchSeries(context.TODO(), nil, nil)
	assert.NoError(t, err, "No error")
	require.NotNil(t, res, "Non empty result")
	assert.Len(t, res.Metrics, 0, "No series")
}

func TestFanoutSearchError(t *testing.T) {
	store := setupFanoutRead(t, true)
	opts := storage.NewFetchOptions()
	_, err := store.SearchSeries(context.TODO(), &storage.FetchQuery{}, opts)
	assert.Error(t, err)
}

func TestFanoutWriteEmpty(t *testing.T) {
	store := setupFanoutWrite(t, false, fmt.Errorf("write error"))
	err := store.Write(context.TODO(), nil)
	assert.NoError(t, err)
}

func TestFanoutWriteError(t *testing.T) {
	store := setupFanoutWrite(t, true, fmt.Errorf("write error"))
	datapoints := make(ts.Datapoints, 1)
	datapoints[0] = ts.Datapoint{Timestamp: xtime.Now(), Value: 1}

	writeQuery, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Datapoints: datapoints,
		Tags:       models.MustMakeTags("foo", "bar"),
		Unit:       xtime.Second,
	})
	require.NoError(t, err)

	assert.Error(t, store.Write(context.TODO(), writeQuery))
}

func TestFanoutWriteSuccess(t *testing.T) {
	store := setupFanoutWrite(t, true, nil)
	datapoints := make(ts.Datapoints, 1)
	datapoints[0] = ts.Datapoint{Timestamp: xtime.Now(), Value: 1}

	writeQuery, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Datapoints: datapoints,
		Tags:       models.MustMakeTags("foo", "bar"),
		Unit:       xtime.Second,
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
	require.NoError(t, err)

	assert.NoError(t, store.Write(context.TODO(), writeQuery))
}

func TestCompleteTagsError(t *testing.T) {
	store := setupFanoutWrite(t, true, fmt.Errorf("err"))
	datapoints := make(ts.Datapoints, 1)
	datapoints[0] = ts.Datapoint{Timestamp: xtime.Now(), Value: 1}
	_, err := store.CompleteTags(
		context.TODO(),
		&storage.CompleteTagsQuery{
			CompleteNameOnly: true,
			TagMatchers:      models.Matchers{},
		},
		storage.NewFetchOptions(),
	)
	assert.Error(t, err)
}

// Error continuation tests below.
func TestFanoutSearchErrorContinues(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filter := func(_ storage.Query, _ storage.Storage) bool { return true }
	tFilter := func(_ storage.CompleteTagsQuery, _ storage.Storage) bool { return true }
	okStore := storage.NewMockStorage(ctrl)
	okStore.EXPECT().SearchSeries(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&storage.SearchResults{
				Metrics: models.Metrics{
					models.Metric{
						ID: []byte("ok"),
						Tags: models.NewTags(1, models.NewTagOptions()).AddTag(models.Tag{
							Name:  []byte("foo"),
							Value: []byte("bar"),
						}),
					},
				},
			},
			nil,
		)

	dupeStore := storage.NewMockStorage(ctrl)
	dupeStore.EXPECT().SearchSeries(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&storage.SearchResults{
				Metrics: models.Metrics{
					models.Metric{
						ID: []byte("ok"),
						Tags: models.NewTags(1, models.NewTagOptions()).AddTag(models.Tag{
							Name:  []byte("foo"),
							Value: []byte("bar"),
						}),
					},
				},
			},
			nil,
		)

	warnStore := storage.NewMockStorage(ctrl)
	warnStore.EXPECT().SearchSeries(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&storage.SearchResults{
				Metrics: models.Metrics{
					models.Metric{
						ID: []byte("warn"),
					},
				},
			},
			errors.New("e"),
		)
	warnStore.EXPECT().ErrorBehavior().Return(storage.BehaviorWarn)
	warnStore.EXPECT().Name().Return("warn").AnyTimes()

	stores := []storage.Storage{warnStore, okStore, dupeStore}
	store := NewStorage(stores, filter, filter, tFilter,
		models.NewTagOptions(), storagem3.NewOptions(encoding.NewOptions()),
		instrument.NewOptions())
	opts := storage.NewFetchOptions()
	result, err := store.SearchSeries(context.TODO(), &storage.FetchQuery{}, opts)
	assert.NoError(t, err)

	require.Equal(t, 1, len(result.Metrics))
	assert.Equal(t, []byte("ok"), result.Metrics[0].ID)
	require.Equal(t, 1, result.Metrics[0].Tags.Len())
	tag := result.Metrics[0].Tags.Tags[0]
	require.Equal(t, []byte("foo"), tag.Name)
	require.Equal(t, []byte("bar"), tag.Value)
}

func TestFanoutCompleteTagsErrorContinues(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filter := func(_ storage.Query, _ storage.Storage) bool { return true }
	tFilter := func(_ storage.CompleteTagsQuery, _ storage.Storage) bool { return true }
	okStore := storage.NewMockStorage(ctrl)
	okStore.EXPECT().CompleteTags(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&consolidators.CompleteTagsResult{
				CompleteNameOnly: true,
				CompletedTags: []consolidators.CompletedTag{
					{
						Name: []byte("ok"),
					},
				},
			},
			nil,
		)

	warnStore := storage.NewMockStorage(ctrl)
	warnStore.EXPECT().CompleteTags(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&consolidators.CompleteTagsResult{
				CompleteNameOnly: true,
				CompletedTags: []consolidators.CompletedTag{
					{
						Name: []byte("warn"),
					},
				},
			},
			errors.New("e"),
		)
	warnStore.EXPECT().ErrorBehavior().Return(storage.BehaviorWarn)
	warnStore.EXPECT().Name().Return("warn").AnyTimes()

	stores := []storage.Storage{warnStore, okStore}
	store := NewStorage(stores, filter, filter, tFilter,
		models.NewTagOptions(), storagem3.NewOptions(encoding.NewOptions()),
		instrument.NewOptions())
	opts := storage.NewFetchOptions()
	q := &storage.CompleteTagsQuery{CompleteNameOnly: true}
	result, err := store.CompleteTags(context.TODO(), q, opts)
	assert.NoError(t, err)

	require.True(t, result.CompleteNameOnly)
	require.Equal(t, 1, len(result.CompletedTags))
	assert.Equal(t, []byte("ok"), result.CompletedTags[0].Name)
}

func TestFanoutFetchBlocksErrorContinues(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filter := func(_ storage.Query, _ storage.Storage) bool { return true }
	tFilter := func(_ storage.CompleteTagsQuery, _ storage.Storage) bool { return true }
	okBlock := block.NewScalar(1, block.Metadata{})
	okStore := storage.NewMockStorage(ctrl)
	okStore.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			block.Result{
				Blocks: []block.Block{okBlock},
			},
			nil,
		)

	warnStore := storage.NewMockStorage(ctrl)
	warnBlock := block.NewScalar(2, block.Metadata{})
	warnStore.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			block.Result{
				Blocks: []block.Block{warnBlock},
			},
			errors.New("e"),
		)
	warnStore.EXPECT().ErrorBehavior().Return(storage.BehaviorWarn)
	warnStore.EXPECT().Name().Return("warn").AnyTimes()

	stores := []storage.Storage{okStore, warnStore}
	store := NewStorage(stores, filter, filter, tFilter,
		models.NewTagOptions(), storagem3.NewOptions(encoding.NewOptions()),
		instrument.NewOptions())
	opts := storage.NewFetchOptions()
	result, err := store.FetchBlocks(context.TODO(), &storage.FetchQuery{}, opts)
	assert.NoError(t, err)

	require.Equal(t, 1, len(result.Blocks))
	assert.Equal(t, block.BlockLazy, result.Blocks[0].Info().Type())
	it, err := result.Blocks[0].StepIter()
	require.NoError(t, err)
	for it.Next() {
		assert.Equal(t, []float64{1}, it.Current().Values())
	}
}

func TestFanoutFetchErrorContinues(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filter := func(_ storage.Query, _ storage.Storage) bool {
		return true
	}

	tFilter := func(_ storage.CompleteTagsQuery, _ storage.Storage) bool {
		return true
	}

	okStore := storage.NewMockStorage(ctrl)
	okStore.EXPECT().FetchCompressed(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			fetchResult("ok"),
			nil,
		).AnyTimes()
	okStore.EXPECT().Type().Return(storage.TypeLocalDC).AnyTimes()

	warnStore := storage.NewMockStorage(ctrl)
	warnStore.EXPECT().FetchCompressed(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			fetchResult("warn"),
			errors.New("e"),
		)
	warnStore.EXPECT().ErrorBehavior().Return(storage.BehaviorWarn)
	warnStore.EXPECT().Name().Return("warn").AnyTimes()

	stores := []storage.Storage{okStore, warnStore}
	store := NewStorage(stores, filter, filter, tFilter,
		models.NewTagOptions(), storagem3.NewOptions(encoding.NewOptions()),
		instrument.NewOptions())
	opts := storage.NewFetchOptions()
	opts.SeriesLimit = 300
	result, err := store.FetchProm(context.TODO(), &storage.FetchQuery{}, opts)
	assert.NoError(t, err)

	series := result.PromResult.GetTimeseries()
	require.Equal(t, 2, len(series))
}

func fetchResult(name string) consolidators.MultiFetchResult {
	it, _ := test.BuildTestSeriesIterator(name)
	iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{it}, nil)
	result := consolidators.NewMultiFetchResult(
		consolidators.NamespaceCoversAllQueryRange,
		nil,
		consolidators.MatchOptions{MatchType: consolidators.MatchTags},
		models.NewTagOptions(),
		consolidators.LimitOptions{
			Limit:             300,
			RequireExhaustive: false,
		},
	)
	result.Add(consolidators.MultiFetchResults{
		SeriesIterators: iters,
		Metadata:        block.NewResultMetadata(),
		Attrs: storagemetadata.Attributes{
			MetricsType: 0,
			Retention:   30,
			Resolution:  30,
		},
		Err: nil,
	})
	return result
}
