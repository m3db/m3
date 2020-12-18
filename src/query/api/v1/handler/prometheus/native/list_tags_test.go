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
	"errors"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type listTagsMatcher struct {
	now time.Time
}

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
	if !q.End.Equal(m.now) {
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

func TestListTags(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testListTags(t, tt.meta, tt.ex)
		})
	}
}

func testListTags(t *testing.T, meta block.ResultMetadata, header string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	storeResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: true,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("bar")},
			{Name: b("baz")},
			{Name: b("foo")},
		},

		Metadata: meta,
	}

	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{Timeout: 15 * time.Second})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetFetchOptionsBuilder(fb).
		SetNowFn(nowFn)
	h := NewListTagsHandler(opts)
	for _, method := range []string{"GET", "POST"} {
		matcher := &listTagsMatcher{now: now}
		store.EXPECT().CompleteTags(gomock.Any(), matcher, gomock.Any()).
			Return(storeResult, nil)

		req := httptest.NewRequest(method, "/labels", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Result().StatusCode)

		body := w.Result().Body
		defer body.Close()

		r, err := ioutil.ReadAll(body)
		require.NoError(t, err)

		ex := `{"status":"success","data":["bar","baz","foo"]}`
		require.Equal(t, ex, string(r))

		actual := w.Header().Get(headers.LimitHeader)
		assert.Equal(t, header, actual)
	}
}

func TestListErrorTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{Timeout: 15 * time.Second})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetFetchOptionsBuilder(fb).
		SetNowFn(nowFn)
	handler := NewListTagsHandler(opts)
	for _, method := range []string{"GET", "POST"} {
		matcher := &listTagsMatcher{now: now}
		store.EXPECT().CompleteTags(gomock.Any(), matcher, gomock.Any()).
			Return(nil, errors.New("err"))

		req := httptest.NewRequest(method, "/labels", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		body := w.Result().Body
		defer body.Close()

		r, err := ioutil.ReadAll(body)
		require.NoError(t, err)

		require.JSONEq(t, `{"status":"error","error":"err"}`, string(r))
	}
}
