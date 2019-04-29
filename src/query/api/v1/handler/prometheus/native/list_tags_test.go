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

package native

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type listTagsMatcher struct{}

func (m *listTagsMatcher) String() string { return "list tags query" }
func (m *listTagsMatcher) Matches(x interface{}) bool {
	q, ok := x.(*storage.CompleteTagsQuery)
	if !ok {
		return false
	}

	if !q.Start.Equal(time.Time{}) {
		return false
	}

	// NB: end time for the query should be roughly `Now`
	diff := q.End.Sub(time.Now())
	absDiff := time.Duration(math.Abs(float64(diff)))
	if absDiff > time.Second {
		return false
	}

	if !q.CompleteNameOnly {
		return false
	}

	if len(q.FilterNameTags) != 0 {
		return false
	}

	if len(q.TagMatchers) != 1 {
		return false
	}

	return models.MatchAll == q.TagMatchers[0].Type
}

var _ gomock.Matcher = &listTagsMatcher{}

func b(s string) []byte { return []byte(s) }

type writer struct {
	results []string
}

var _ http.ResponseWriter = &writer{}

func (w *writer) WriteHeader(_ int)   {}
func (w *writer) Header() http.Header { return make(http.Header) }
func (w *writer) Write(b []byte) (int, error) {
	if w.results == nil {
		w.results = make([]string, 0, 10)
	}

	w.results = append(w.results, string(b))
	return len(b), nil
}

type result struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func TestListTags(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	storeResult := &storage.CompleteTagsResult{
		CompleteNameOnly: true,
		CompletedTags: []storage.CompletedTag{
			{Name: b("bar")},
			{Name: b("baz")},
			{Name: b("foo")},
		},
	}

	store.EXPECT().CompleteTags(gomock.Any(), &listTagsMatcher{}, gomock.Any()).
		Return(storeResult, nil)

	handler := NewListTagsHandler(store)

	// execute the query
	w := &writer{}
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "",
		},
	}

	handler.ServeHTTP(w, req)
	require.Equal(t, 1, len(w.results))
	var r result
	json.Unmarshal([]byte(w.results[0]), &r)

	ex := result{
		Status: "success",
		Data:   []string{"bar", "baz", "foo"},
	}

	require.Equal(t, ex, r)
}

type errResult struct {
	Error string `json:"error"`
}

func TestListErrorTags(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	store.EXPECT().CompleteTags(gomock.Any(), &listTagsMatcher{}, gomock.Any()).
		Return(nil, errors.New("err"))
	handler := NewListTagsHandler(store)

	// execute the query
	w := &writer{}
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "",
		},
	}

	handler.ServeHTTP(w, req)
	require.Equal(t, 1, len(w.results))
	var r errResult
	json.Unmarshal([]byte(w.results[0]), &r)

	ex := errResult{
		Error: "err",
	}

	require.Equal(t, ex, r)
}
