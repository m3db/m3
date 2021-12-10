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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/headers"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tagValuesMatcher struct {
	start, end xtime.UnixNano
	filterTag  string
}

func (m *tagValuesMatcher) String() string { return "tag values query" }
func (m *tagValuesMatcher) Matches(x interface{}) bool {
	q, ok := x.(*storage.CompleteTagsQuery)
	if !ok {
		return false
	}

	if !q.Start.Equal(m.start) {
		return false
	}

	if !q.End.Equal(m.end) {
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
	now := xtime.Now()
	nowFn := func() time.Time {
		return now.ToTime()
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetNowFn(nowFn).
		SetTagOptions(models.NewTagOptions()).
		SetFetchOptionsBuilder(fb)

	valueHandler := NewTagValuesHandler(opts)
	names := []struct {
		name string
	}{
		{"up"},
		{"__name__"},
	}

	for _, tt := range names {
		testTagValuesWithMatch(t, now, store, tt.name, valueHandler, false)
		testTagValuesWithMatch(t, now, store, tt.name, valueHandler, true)
	}
}

func testTagValuesWithMatch(
	t *testing.T,
	now xtime.UnixNano,
	store *storage.MockStorage,
	name string,
	valueHandler http.Handler,
	withMatchOverride bool,
) {
	path := fmt.Sprintf("%s/label/%s/values?start=100", route.Prefix, name)
	nameMatcher := models.Matcher{
		Type: models.MatchField,
		Name: []byte(name),
	}
	matchers := models.Matchers{nameMatcher}
	if withMatchOverride {
		path = fmt.Sprintf("%s/label/%s/values?start=100&match[]=testing", route.Prefix, name)
		matchers = models.Matchers{
			nameMatcher,
			{
				Type:  models.MatchEqual,
				Name:  []byte("__name__"),
				Value: []byte("testing"),
			},
		}
	}

	matcher := &storage.CompleteTagsQuery{
		Start:            xtime.FromSeconds(100),
		End:              now,
		CompleteNameOnly: false,
		FilterNameTags:   [][]byte{[]byte(name)},
		TagMatchers:      matchers,
	}

	// nolint:noctx
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	router := mux.NewRouter()

	storeResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{
				Name:   b(name),
				Values: bs("a", "b", "c", name),
			},
		},
		Metadata: block.ResultMetadata{
			Exhaustive:     false,
			Warnings:       []block.Warning{{Name: "foo", Message: "bar"}},
			MetadataByName: make(map[string]*block.ResultMetricMetadata),
		},
	}

	store.EXPECT().CompleteTags(gomock.Any(), gomock.Eq(matcher), gomock.Any()).
		Return(storeResult, nil)

	router.HandleFunc(TagValuesURL, valueHandler.ServeHTTP)
	router.ServeHTTP(rr, req)

	read, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	ex := fmt.Sprintf(`{"status":"success","data":["a","b","c","%s"]}`, name)
	assert.Equal(t, ex, string(read))

	warning := rr.Header().Get(headers.LimitHeader)
	exWarn := fmt.Sprintf("%s,foo_bar", headers.LimitHeaderSeriesLimitApplied)
	assert.Equal(t, exWarn, warning)
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

//nolint:dupl
func TestTagValueTimeout(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	req := httptest.NewRequest("GET", TagValuesURL, nil)
	w := httptest.NewRecorder()
	h := NewTagValuesHandler(storageSetup(t, ctrl, 1*time.Millisecond, expectTimeout))
	router := mux.NewRouter()
	router.HandleFunc(
		fmt.Sprintf("%s/label/{%s}/values", route.Prefix, route.NameReplace),
		h.ServeHTTP,
	)
	router.ServeHTTP(w, req)

	assert.Equal(t, 504, w.Code, "Status code not 504")
	assert.Contains(t, w.Body.String(), "context deadline exceeded")
}

//nolint:dupl
func TestTagValueUseRequestContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("GET", TagValuesURL, nil).WithContext(cancelledCtx)
	w := httptest.NewRecorder()
	h := NewTagValuesHandler(storageSetup(t, ctrl, 15*time.Second, expectCancellation))
	router := mux.NewRouter()
	router.HandleFunc(
		fmt.Sprintf("%s/label/{%s}/values", route.Prefix, route.NameReplace),
		h.ServeHTTP,
	)
	router.ServeHTTP(w, req)

	assert.Equal(t, 499, w.Code, "Status code not 499")
	assert.Contains(t, w.Body.String(), "context canceled")
}

func storageSetup(
	t *testing.T,
	ctrl *gomock.Controller,
	timeout time.Duration,
	fn completeTagsFn,
) options.HandlerOptions {
	// Setup storage
	store := storage.NewMockStorage(ctrl)
	store.EXPECT().CompleteTags(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(fn)

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{Timeout: timeout})
	require.NoError(t, err)
	return options.EmptyHandlerOptions().
		SetStorage(store).
		SetFetchOptionsBuilder(fb)
}

type completeTagsFn func(
	context.Context,
	*storage.CompleteTagsQuery,
	*storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error)

func expectCancellation(
	ctx context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, ctx.Err()
	}
	return nil, nil
}

func expectTimeout(
	ctx context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	<-ctx.Done()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, ctx.Err()
	}
	return nil, nil
}
