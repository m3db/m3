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
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/headers"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildWarningMeta(name, message string) block.ResultMetadata {
	meta := block.NewResultMetadata()
	meta.AddWarning(name, message)
	return meta
}

var tests = []struct {
	name string
	meta block.ResultMetadata
	ex   string
}{
	{"complete", block.NewResultMetadata(), ""},
	{
		"non-exhaustive",
		block.ResultMetadata{Exhaustive: false},
		headers.LimitHeaderSeriesLimitApplied,
	},
	{
		"warnings",
		buildWarningMeta("foo", "bar"),
		"foo_bar",
	},
}

func TestCompleteTags(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCompleteTags(t, tt.meta, tt.ex)
		})
	}
}

func testCompleteTags(t *testing.T, meta block.ResultMetadata, header string) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	storeResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("bar"), Values: [][]byte{b("qux")}},
			{Name: b("baz")},
			{Name: b("foo")},
		},

		Metadata: meta,
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{Timeout: 15 * time.Second})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetFetchOptionsBuilder(fb)
	h := NewCompleteTagsHandler(opts)
	store.EXPECT().CompleteTags(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(storeResult, nil)

	req := httptest.NewRequest("GET", "/search?query=foo", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)
	body := w.Result().Body
	defer body.Close()

	r, err := ioutil.ReadAll(body)
	require.NoError(t, err)

	ex := `{"hits":3,"tags":[{"key":"bar","values":["qux"]},` +
		`{"key":"baz","values":[]},{"key":"foo","values":[]}]}`
	require.Equal(t, ex, string(r))

	actual := w.Header().Get(headers.LimitHeader)
	assert.Equal(t, header, actual)
}

var _ gomock.Matcher = (*completeTagsMatcher)(nil)

type completeTagsMatcher struct {
	name string
}

func (c *completeTagsMatcher) Matches(x interface{}) bool {
	q, ok := x.(*storage.CompleteTagsQuery)
	if !ok {
		return false
	}

	if q.CompleteNameOnly {
		return false
	}

	if len(q.TagMatchers) != 1 {
		return false
	}

	return bytes.Equal([]byte(c.name), q.TagMatchers[0].Name)
}

func (c *completeTagsMatcher) String() string { return "complete tags matcher" }

func TestMultiCompleteTags(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := storage.NewMockStorage(ctrl)
	fooMeta := block.NewResultMetadata()
	fooMeta.Exhaustive = false
	fooResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("bar"), Values: [][]byte{b("zulu"), b("quail")}},
			{Name: b("foo"), Values: [][]byte{b("quail")}},
		},

		Metadata: fooMeta,
	}

	barMeta := block.NewResultMetadata()
	barMeta.AddWarning("abc", "def")
	barResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("bar"), Values: [][]byte{b("qux")}},
		},

		Metadata: barMeta,
	}

	fb, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{Timeout: 15 * time.Second})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetStorage(store).
		SetFetchOptionsBuilder(fb)

	store.EXPECT().CompleteTags(gomock.Any(), &completeTagsMatcher{name: "foo"},
		gomock.Any()).Return(fooResult, nil)

	store.EXPECT().CompleteTags(gomock.Any(), &completeTagsMatcher{name: "bar"},
		gomock.Any()).Return(barResult, nil)

	req := httptest.NewRequest("GET", "/search?query=foo&query=bar", nil)
	w := httptest.NewRecorder()

	h := NewCompleteTagsHandler(opts)
	h.ServeHTTP(w, req)
	body := w.Result().Body
	defer body.Close()

	r, err := ioutil.ReadAll(body)
	require.NoError(t, err)

	ex := `{"hits":2,"tags":[{"key":"bar","values":["quail","qux","zulu"]},` +
		`{"key":"foo","values":["quail"]}]}`
	require.Equal(t, ex, string(r))

	actual := w.Header().Get(headers.LimitHeader)
	assert.Equal(t, "max_fetch_series_limit_applied,abc_def", actual)
}

//nolint:dupl
func TestCompleteTagsKillOnTimeout(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	req := httptest.NewRequest("GET", "/search?query=foo", nil)
	w := httptest.NewRecorder()
	h := NewCompleteTagsHandler(storageSetup(t, ctrl, 1*time.Millisecond, expectTimeout))
	h.ServeHTTP(w, req)

	assert.Equal(t, 500, w.Code, "Status code not 500")
	assert.Contains(t, w.Body.String(), "context deadline exceeded")
}

//nolint:dupl
func TestCompleteTagsUseRequestContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("GET", "/search?query=foo", nil).WithContext(cancelledCtx)
	w := httptest.NewRecorder()
	h := NewCompleteTagsHandler(storageSetup(t, ctrl, 15*time.Second, expectCancellation))
	h.ServeHTTP(w, req)

	assert.Equal(t, 500, w.Code, "Status code not 500")
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
