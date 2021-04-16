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

package graphite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
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

// dates is a tuple of a date with a valid string representation
type date struct {
	t time.Time
	s string
}

var (
	from = date{
		s: "14:38_20150618",
		t: time.Date(2015, time.June, 18, 14, 38, 0, 0, time.UTC),
	}
	until = date{
		s: "1432581620",
		t: time.Date(2015, time.May, 25, 19, 20, 20, 0, time.UTC),
	}
)

type completeTagQueryMatcher struct {
	matchers []models.Matcher
}

func (m *completeTagQueryMatcher) String() string { return "complete tag query" }
func (m *completeTagQueryMatcher) Matches(x interface{}) bool {
	q, ok := x.(*storage.CompleteTagsQuery)
	if !ok {
		return false
	}

	if !q.Start.Equal(from.t) {
		return false
	}

	if !q.End.Equal(until.t) {
		return false
	}

	if q.CompleteNameOnly {
		return false
	}

	if len(q.FilterNameTags) != 1 {
		return false
	}

	// both queries should filter on __g1__
	if !bytes.Equal(q.FilterNameTags[0], []byte("__g1__")) {
		return false
	}

	if len(q.TagMatchers) != len(m.matchers) {
		return false
	}

	for i, qMatcher := range q.TagMatchers {
		if !bytes.Equal(qMatcher.Name, m.matchers[i].Name) {
			return false
		}
		if !bytes.Equal(qMatcher.Value, m.matchers[i].Value) {
			return false
		}
		if qMatcher.Type != m.matchers[i].Type {
			return false
		}
	}

	return true
}

var _ gomock.Matcher = &completeTagQueryMatcher{}

func b(s string) []byte { return []byte(s) }
func bs(ss ...string) [][]byte {
	bb := make([][]byte, len(ss))
	for i, s := range ss {
		bb[i] = b(s)
	}

	return bb
}

func setupStorage(ctrl *gomock.Controller, ex, ex2 bool) storage.Storage {
	store := storage.NewMockStorage(ctrl)
	// set up no children case
	noChildrenMatcher := &completeTagQueryMatcher{
		matchers: []models.Matcher{
			{Type: models.MatchEqual, Name: b("__g0__"), Value: b("foo")},
			{Type: models.MatchRegexp, Name: b("__g1__"), Value: b(`b[^\.]*`)},
			{Type: models.MatchNotField, Name: b("__g2__")},
		},
	}

	noChildrenResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("__g1__"), Values: bs("bug", "bar", "baz")},
		},
		Metadata: block.ResultMetadata{
			LocalOnly:  true,
			Exhaustive: ex,
		},
	}

	store.EXPECT().CompleteTags(gomock.Any(), noChildrenMatcher, gomock.Any()).
		Return(noChildrenResult, nil)

	// set up children case
	childrenMatcher := &completeTagQueryMatcher{
		matchers: []models.Matcher{
			{Type: models.MatchEqual, Name: b("__g0__"), Value: b("foo")},
			{Type: models.MatchRegexp, Name: b("__g1__"), Value: b(`b[^\.]*`)},
			{Type: models.MatchField, Name: b("__g2__")},
		},
	}

	childrenResult := &consolidators.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags: []consolidators.CompletedTag{
			{Name: b("__g1__"), Values: bs("baz", "bix", "bug")},
		},
		Metadata: block.ResultMetadata{
			LocalOnly:  false,
			Exhaustive: true,
		},
	}

	if !ex2 {
		childrenResult.Metadata.AddWarning("foo", "bar")
	}

	store.EXPECT().CompleteTags(gomock.Any(), childrenMatcher, gomock.Any()).
		Return(childrenResult, nil)

	return store
}

type writer struct {
	results []string
	header  http.Header
}

var _ http.ResponseWriter = &writer{}

func (w *writer) WriteHeader(_ int) {}
func (w *writer) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}

	return w.header
}

func (w *writer) Write(b []byte) (int, error) {
	if w.results == nil {
		w.results = make([]string, 0, 10)
	}

	w.results = append(w.results, string(b))
	return len(b), nil
}

type result struct {
	ID            string `json:"id"`
	Text          string `json:"text"`
	Leaf          int    `json:"leaf"`
	Expandable    int    `json:"expandable"`
	AllowChildren int    `json:"allowChildren"`
}

type results []result

func (r results) Len() int      { return len(r) }
func (r results) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r results) Less(i, j int) bool {
	return strings.Compare(r[i].ID, r[j].ID) == -1
}

func testFind(t *testing.T, httpMethod string, ex bool, ex2 bool, header string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// setup storage and handler
	store := setupStorage(ctrl, ex, ex2)

	builder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts := options.EmptyHandlerOptions().
		SetGraphiteFindFetchOptionsBuilder(builder).
		SetStorage(store)
	h := NewFindHandler(opts)

	// execute the query
	params := make(url.Values)
	params.Set("query", "foo.b*")
	params.Set("from", from.s)
	params.Set("until", until.s)

	w := &writer{}
	req := &http.Request{
		Method: httpMethod,
	}
	switch httpMethod {
	case http.MethodGet:
		req.URL = &url.URL{
			RawQuery: params.Encode(),
		}
	case http.MethodPost:
		req.Form = params
	}

	h.ServeHTTP(w, req)

	// convert results to comparable format
	require.Equal(t, 1, len(w.results))
	r := make(results, 0)
	decoder := json.NewDecoder(bytes.NewBufferString((w.results[0])))
	require.NoError(t, decoder.Decode(&r))
	sort.Sort(r)

	makeNoChildrenResult := func(t string) result {
		return result{ID: fmt.Sprintf("foo.%s", t), Text: t, Leaf: 1,
			Expandable: 0, AllowChildren: 0}
	}

	makeWithChildrenResult := func(t string) result {
		return result{ID: fmt.Sprintf("foo.%s", t), Text: t, Leaf: 0,
			Expandable: 1, AllowChildren: 1}
	}

	expected := results{
		makeNoChildrenResult("bar"),
		makeNoChildrenResult("baz"),
		makeWithChildrenResult("baz"),
		makeWithChildrenResult("bix"),
		makeNoChildrenResult("bug"),
		makeWithChildrenResult("bug"),
	}

	require.Equal(t, expected, r)
	actual := w.Header().Get(headers.LimitHeader)
	assert.Equal(t, header, actual)
}

var limitTests = []struct {
	name    string
	ex, ex2 bool
	header  string
}{
	{"both incomplete", false, false, fmt.Sprintf(
		"%s,%s_%s", headers.LimitHeaderSeriesLimitApplied, "foo", "bar")},
	{"with terminator incomplete", true, false, "foo_bar"},
	{"with children incomplete", false, true,
		headers.LimitHeaderSeriesLimitApplied},
	{"both complete", true, true, ""},
}

func TestFind(t *testing.T) {
	for _, tt := range limitTests {
		t.Run(tt.name, func(t *testing.T) {
			for _, httpMethod := range FindHTTPMethods {
				testFind(t, httpMethod, tt.ex, tt.ex2, tt.header)
			}
		})
	}
}
