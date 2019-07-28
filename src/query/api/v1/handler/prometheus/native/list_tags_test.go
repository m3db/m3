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
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"

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

func TestListTagsNotExhaustive(t *testing.T) {
	testListTags(t, false)
}

func TestListTagsExhaustive(t *testing.T) {
	testListTags(t, true)
}

func testListTags(t *testing.T, exhaustive bool) {
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
		Exhaustive: exhaustive,
	}

	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	h := NewListTagsHandler(store,
		handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}),
		nowFn, instrument.NewOptions())
	for _, method := range []string{"GET", "POST"} {
		matcher := &listTagsMatcher{now: now}
		store.EXPECT().CompleteTags(gomock.Any(), matcher, gomock.Any()).
			Return(storeResult, nil)

		req := httptest.NewRequest(method, "/labels", nil)

		h.ServeHTTP(w, req)
		body := w.Result().Body
		defer body.Close()

		r, err := ioutil.ReadAll(body)
		require.NoError(t, err)

		ex := `{"status":"success","data":["bar","baz","foo"]}`
		require.Equal(t, ex, string(r))

		header := w.Header().Get(handler.LimitHeader)
		if exhaustive {
			assert.Equal(t, "", header)
		} else {
			assert.Equal(t, "true", header)
		}
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

	handler := NewListTagsHandler(store,
		handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}),
		nowFn, instrument.NewOptions())
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

		ex := `{"error":"err"}`
		// NB: error handler adds a newline to the output.
		ex = fmt.Sprintf("%s\n", ex)
		require.Equal(t, ex, string(r))
	}
}
