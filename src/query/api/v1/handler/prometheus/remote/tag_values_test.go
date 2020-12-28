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

package remote

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/headers"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tagValuesMatcher struct {
	now       time.Time
	filterTag string
}

func (m *tagValuesMatcher) String() string { return "tag values query" }
func (m *tagValuesMatcher) Matches(x interface{}) bool {
	q, ok := x.(*storage.CompleteTagsQuery)
	if !ok {
		return false
	}

	if !q.Start.Equal(time.Time{}) {
		return false
	}

	if !q.End.Equal(m.now) {
		return false
	}

	if q.CompleteNameOnly {
		return false
	}

	if len(q.FilterNameTags) != 1 {
		return false
	}

	if len(q.TagMatchers) != 1 {
		return false
	}

	tm := q.TagMatchers[0]
	return models.MatchField == tm.Type &&
		bytes.Equal([]byte(m.filterTag), tm.Name)
}

var _ gomock.Matcher = &tagValuesMatcher{}

func b(s string) []byte { return []byte(s) }
func bs(ss ...string) [][]byte {
	bb := make([][]byte, len(ss))
	for i, s := range ss {
		bb[i] = b(s)
	}

	return bb
}

func TestTagValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetNowFn(nowFn).
		SetFetchOptionsBuilder(fb)

	valueHandler := NewTagValuesHandler(opts)
	names := []struct {
		name string
	}{
		{"up"},
		{"__name__"},
	}
	url := fmt.Sprintf("/label/{%s}/values", NameReplace)

	for _, tt := range names {
		path := fmt.Sprintf("/label/%s/values", tt.name)
		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		router := mux.NewRouter()
		matcher := &tagValuesMatcher{
			now:       now,
			filterTag: tt.name,
		}

		storeResult := &consolidators.CompleteTagsResult{
			CompleteNameOnly: false,
			CompletedTags: []consolidators.CompletedTag{
				{
					Name:   b(tt.name),
					Values: bs("a", "b", "c", tt.name),
				},
			},
			Metadata: block.ResultMetadata{
				Exhaustive: false,
				Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
			},
		}

		store.EXPECT().CompleteTags(gomock.Any(), matcher, gomock.Any()).
			Return(storeResult, nil)

		router.HandleFunc(url, valueHandler.ServeHTTP)
		router.ServeHTTP(rr, req)

		read, err := ioutil.ReadAll(rr.Body)
		require.NoError(t, err)

		ex := fmt.Sprintf(`{"status":"success","data":["a","b","c","%s"]}`, tt.name)
		assert.Equal(t, ex, string(read))

		warning := rr.Header().Get(headers.LimitHeader)
		exWarn := fmt.Sprintf("%s,foo_bar", headers.LimitHeaderSeriesLimitApplied)
		assert.Equal(t, exWarn, warning)
	}
}

func TestTagValueErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetNowFn(nowFn).
		SetFetchOptionsBuilder(fb)

	handler := NewTagValuesHandler(opts)
	url := "/label"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	router := mux.NewRouter()
	router.HandleFunc(url, handler.ServeHTTP)
	router.ServeHTTP(rr, req)

	read, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	ex := `{"status":"error","error":"invalid path with no name present"}`
	assert.JSONEq(t, ex, string(read))
}
