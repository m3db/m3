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

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testID        = "test_id"
	testNamespace = "test_namespace"
)

func generateSearchReq() *storage.FetchQuery {
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

func generateSearchBody(t *testing.T) io.Reader {
	req := generateSearchReq()
	data, err := json.Marshal(req)
	require.NoError(t, err)

	return bytes.NewReader(data)
}

func generateQueryResults(tagsIter index.Iterator) index.QueryResults {
	return index.QueryResults{
		Iterator: tagsIter,
	}
}

func generateTag() ident.Tag {
	return ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}
}

func generateTagIters(ctrl *gomock.Controller) *index.MockIterator {
	mockTaggedIDsIter := index.NewMockIterator(ctrl)
	mockTaggedIDsIter.EXPECT().Next().Return(true).MaxTimes(1)
	mockTaggedIDsIter.EXPECT().Next().Return(false)
	mockTaggedIDsIter.EXPECT().Current().Return(ident.StringID(testNamespace), ident.StringID(testID), []ident.Tag{generateTag()})

	return mockTaggedIDsIter
}

func searchServer(t *testing.T) *SearchHandler {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)

	mockTaggedIDsIter := generateTagIters(ctrl)

	session := client.NewMockSession(ctrl)
	session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any()).Return(generateQueryResults(mockTaggedIDsIter), nil)

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	search := &SearchHandler{store: storage}

	return search
}

func TestSearchResponse(t *testing.T) {
	searchHandler := searchServer(t)

	opts := newFetchOptions(100)
	results, err := searchHandler.search(context.TODO(), generateSearchReq(), &opts)
	require.NoError(t, err)

	assert.Equal(t, testID, results.Metrics[0].ID)
	assert.Equal(t, testNamespace, results.Metrics[0].Namespace)
	assert.Equal(t, models.Tags{"foo": "bar"}, results.Metrics[0].Tags)
}

func TestSearchEndpoint(t *testing.T) {
	searchHandler := searchServer(t)
	server := httptest.NewServer(searchHandler)
	defer server.Close()

	urlWithLimit := fmt.Sprintf("%s%s", server.URL, "?limit=90")
	req, _ := http.NewRequest("POST", urlWithLimit, generateSearchBody(t))
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
}
