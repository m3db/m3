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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/local"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRetention = 30 * 24 * time.Hour

type testSessions struct {
	unaggregated1MonthRetention                *client.MockSession
	aggregated1MonthRetention1MinuteResolution *client.MockSession
}

func setup(
	t *testing.T,
	ctrl *gomock.Controller,
) (*localStorage, testSessions) {
	logging.InitWithCores(nil)
	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
	unaggregated1MonthRetention := client.NewMockSession(ctrl)
	aggregated1MonthRetention1MinuteResolution := client.NewMockSession(ctrl)
	clusters, err := local.NewClusters(local.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unaggregated"),
		Session:     unaggregated1MonthRetention,
		Retention:   testRetention,
	}, local.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_aggregated"),
		Session:     aggregated1MonthRetention1MinuteResolution,
		Retention:   testRetention,
		Resolution:  time.Minute,
	})
	require.NoError(t, err)
	storage := newStorage(clusters, nil)
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

func TestLocalRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store, sessions := setup(t, ctrl)
	iter, err := test.BuildTestSeriesIterator()
	require.NoError(t, err)
	iterators := encoding.NewSeriesIterators([]encoding.SeriesIterator{iter}, nil)
	sessions.unaggregated1MonthRetention.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(iterators, true, nil)
	searchReq := newFetchReq()
	results, err := store.fetchBlocks(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.NoError(t, err)

	for id, seriesBlocks := range results {
		assert.Equal(t, "id", id.String())
		for _, blocks := range seriesBlocks {
			assert.Equal(t, "namespace", blocks.Namespace.String())
			blockTags, err := storage.FromIdentTagIteratorToTags(blocks.Tags)
			require.NoError(t, err)
			s, _ := blockTags.Get("foo")
			assert.Equal(t, "bar", s)
			s, _ = blockTags.Get("baz")
			assert.Equal(t, "qux", s)
		}
	}
}
