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

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/x/ident"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testID = "test_id"
)

func generateSearchReq() *storage.FetchQuery {
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

func generateSearchBody(t *testing.T) io.Reader {
	req := generateSearchReq()
	data, err := json.Marshal(req)
	require.NoError(t, err)

	return bytes.NewReader(data)
}

func generateTagIters(ctrl *gomock.Controller) *client.MockTaggedIDsIterator {
	mockTaggedIDsIter := client.NewMockTaggedIDsIterator(ctrl)
	mockTaggedIDsIter.EXPECT().Next().Return(true).MaxTimes(1)
	mockTaggedIDsIter.EXPECT().Next().Return(false)
	mockTaggedIDsIter.EXPECT().Current().Return(ident.StringID("ns"),
		ident.StringID(testID), seriesiter.GenerateSingleSampleTagIterator(ctrl, seriesiter.GenerateTag()))
	mockTaggedIDsIter.EXPECT().Err().Return(nil).MinTimes(1)
	mockTaggedIDsIter.EXPECT().Finalize()

	return mockTaggedIDsIter
}

func searchServer(t *testing.T) *SearchHandler {
	ctrl := xtest.NewController(t)

	mockTaggedIDsIter := generateTagIters(ctrl)

	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTaggedIDsIter, client.FetchResponseMetadata{Exhaustive: false}, nil).AnyTimes()

	builder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(storage).SetFetchOptionsBuilder(builder)
	search := NewSearchHandler(opts)
	h, ok := search.(*SearchHandler)
	require.True(t, ok)
	return h
}

func TestSearchResponse(t *testing.T) {
	searchHandler := searchServer(t)

	opts := storage.NewFetchOptions()
	opts.SeriesLimit = 100
	results, err := searchHandler.search(context.TODO(), generateSearchReq(), opts)
	require.NoError(t, err)

	assert.Equal(t, []byte(testID), results.Metrics[0].ID)
	expected := test.TagSliceToTags([]models.Tag{{Name: []byte("foo"), Value: []byte("bar")}})
	assert.Equal(t, expected.Tags, results.Metrics[0].Tags.Tags)
}

func TestSearchEndpoint(t *testing.T) {
	searchHandler := searchServer(t)
	server := httptest.NewServer(searchHandler)
	defer server.Close()

	urlWithLimit := fmt.Sprintf("%s%s", server.URL, "?limit=90")
	req, _ := http.NewRequest("POST", urlWithLimit, generateSearchBody(t))
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
}
