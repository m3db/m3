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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/headers"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

// dates is a tuple of a date with a valid string representation
type date struct {
	t xtime.UnixNano
	s string
}

var (
	from = date{
		s: "14:38_20150618",
		t: xtime.ToUnixNano(time.Date(2015, time.June, 18, 14, 38, 0, 0, time.UTC)),
	}
	until = date{
		s: "1432581620",
		t: xtime.ToUnixNano(time.Date(2015, time.May, 25, 19, 20, 20, 0, time.UTC)),
	}
)

type completeTagQueryMatcher struct {
	matchers                 []models.Matcher
	filterNameTagsIndexStart int
	filterNameTagsIndexEnd   int
}

func (m *completeTagQueryMatcher) String() string {
	q := storage.CompleteTagsQuery{
		TagMatchers: m.matchers,
	}
	return q.String()
}

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

	if m.filterNameTagsIndexStart == 0 && m.filterNameTagsIndexEnd == 0 {
		// Default query completing single graphite path index value.
		if len(q.FilterNameTags) != 1 {
			return false
		}

		// Both queries should filter on __g1__.
		if !bytes.Equal(q.FilterNameTags[0], []byte("__g1__")) {
			return false
		}
	} else {
		// Unterminated query completing many grapth path index values.
		n := m.filterNameTagsIndexEnd
		expected := make([][]byte, 0, n)
		for i := m.filterNameTagsIndexStart; i < m.filterNameTagsIndexEnd; i++ {
			expected = append(expected, graphite.TagName(i))
		}

		if len(q.FilterNameTags) != len(expected) {
			return false
		}

		for i := range expected {
			if !bytes.Equal(q.FilterNameTags[i], expected[i]) {
				return false
			}
		}
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

func makeNoChildrenResult(id, text string) result {
	return result{
		ID:            id,
		Text:          text,
		Leaf:          1,
		Expandable:    0,
		AllowChildren: 0,
	}
}

func makeWithChildrenResult(id, text string) result {
	return result{
		ID:            id,
		Text:          text,
		Leaf:          0,
		Expandable:    1,
		AllowChildren: 1,
	}
}

type limitTest struct {
	name    string
	ex, ex2 bool
	header  string
}

var (
	bothCompleteLimitTest = limitTest{"both complete", true, true, ""}
	limitTests            = []limitTest{
		bothCompleteLimitTest,
		{
			"both incomplete", false, false,
			fmt.Sprintf("%s,%s_%s", headers.LimitHeaderSeriesLimitApplied, "foo", "bar"),
		},
		{
			"with terminator incomplete", true, false,
			"foo_bar",
		},
		{
			"with children incomplete", false, true,
			headers.LimitHeaderSeriesLimitApplied,
		},
	}
)

func TestFind(t *testing.T) {
	for _, httpMethod := range FindHTTPMethods {
		testFind(t, testFindOptions{
			httpMethod: httpMethod,
		})
	}
}

type testFindOptions struct {
	httpMethod string
}

type testFindQuery struct {
	expectMatchers *completeTagQueryMatcher
	mockResult     func(lt limitTest) *consolidators.CompleteTagsResult
}

func testFind(t *testing.T, opts testFindOptions) {
	warningsFooBar := block.Warnings{
		block.Warning{
			Name:    "foo",
			Message: "bar",
		},
	}

	for _, test := range []struct {
		query                                             string
		limitTests                                        []limitTest
		terminatedQuery                                   *testFindQuery
		childQuery                                        *testFindQuery
		expectedResultsWithoutExpandableAndLeafDuplicates results
		expectedResultsWithExpandableAndLeafDuplicates    results
	}{
		{
			query:      "foo.b*",
			limitTests: limitTests,
			terminatedQuery: &testFindQuery{
				expectMatchers: &completeTagQueryMatcher{
					matchers: []models.Matcher{
						{Type: models.MatchEqual, Name: b("__g0__"), Value: b("foo")},
						{Type: models.MatchRegexp, Name: b("__g1__"), Value: b(`b[^\.]*`)},
						{Type: models.MatchNotField, Name: b("__g2__")},
					},
				},
				mockResult: func(lt limitTest) *consolidators.CompleteTagsResult {
					return &consolidators.CompleteTagsResult{
						CompleteNameOnly: false,
						CompletedTags: []consolidators.CompletedTag{
							{Name: b("__g1__"), Values: bs("bug", "bar", "baz")},
						},
						Metadata: block.ResultMetadata{
							LocalOnly:      true,
							Exhaustive:     lt.ex,
							MetadataByName: make(map[string]*block.ResultMetricMetadata),
						},
					}
				},
			},
			childQuery: &testFindQuery{
				expectMatchers: &completeTagQueryMatcher{
					matchers: []models.Matcher{
						{Type: models.MatchEqual, Name: b("__g0__"), Value: b("foo")},
						{Type: models.MatchRegexp, Name: b("__g1__"), Value: b(`b[^\.]*`)},
						{Type: models.MatchField, Name: b("__g2__")},
					},
				},
				mockResult: func(lt limitTest) *consolidators.CompleteTagsResult {
					var warnings block.Warnings
					if !lt.ex2 {
						warnings = warningsFooBar
					}
					return &consolidators.CompleteTagsResult{
						CompleteNameOnly: false,
						CompletedTags: []consolidators.CompletedTag{
							{Name: b("__g1__"), Values: bs("baz", "bix", "bug")},
						},
						Metadata: block.ResultMetadata{
							LocalOnly:      false,
							Exhaustive:     true,
							Warnings:       warnings,
							MetadataByName: make(map[string]*block.ResultMetricMetadata),
						},
					}
				},
			},
			expectedResultsWithoutExpandableAndLeafDuplicates: results{
				makeNoChildrenResult("foo.bar", "bar"),
				makeWithChildrenResult("foo.baz", "baz"),
				makeWithChildrenResult("foo.bix", "bix"),
				makeWithChildrenResult("foo.bug", "bug"),
			},
			expectedResultsWithExpandableAndLeafDuplicates: results{
				makeNoChildrenResult("foo.bar", "bar"),
				makeNoChildrenResult("foo.baz", "baz"),
				makeWithChildrenResult("foo.baz", "baz"),
				makeWithChildrenResult("foo.bix", "bix"),
				makeNoChildrenResult("foo.bug", "bug"),
				makeWithChildrenResult("foo.bug", "bug"),
			},
		},
		{
			query: "foo.**.*",
			childQuery: &testFindQuery{
				expectMatchers: &completeTagQueryMatcher{
					matchers: []models.Matcher{
						{
							Type: models.MatchRegexp,
							Name: b("__g0__"), Value: b(".*"),
						},
						{
							Type:  models.MatchRegexp,
							Name:  doc.IDReservedFieldName,
							Value: b(`foo\.+.*[^\.]*`),
						},
					},
					filterNameTagsIndexStart: 2,
					filterNameTagsIndexEnd:   102,
				},
				mockResult: func(_ limitTest) *consolidators.CompleteTagsResult {
					return &consolidators.CompleteTagsResult{
						CompleteNameOnly: false,
						CompletedTags: []consolidators.CompletedTag{
							{Name: b("__g2__"), Values: bs("bar0", "bar1")},
							{Name: b("__g3__"), Values: bs("baz0", "baz1", "baz2")},
						},
						Metadata: block.ResultMetadata{
							LocalOnly:      true,
							Exhaustive:     true,
							MetadataByName: make(map[string]*block.ResultMetricMetadata),
						},
					}
				},
			},
			expectedResultsWithoutExpandableAndLeafDuplicates: results{
				makeWithChildrenResult("foo.**.bar0", "bar0"),
				makeWithChildrenResult("foo.**.bar1", "bar1"),
				makeWithChildrenResult("foo.**.baz0", "baz0"),
				makeWithChildrenResult("foo.**.baz1", "baz1"),
				makeWithChildrenResult("foo.**.baz2", "baz2"),
			},
			expectedResultsWithExpandableAndLeafDuplicates: results{
				makeWithChildrenResult("foo.**.bar0", "bar0"),
				makeWithChildrenResult("foo.**.bar1", "bar1"),
				makeWithChildrenResult("foo.**.baz0", "baz0"),
				makeWithChildrenResult("foo.**.baz1", "baz1"),
				makeWithChildrenResult("foo.**.baz2", "baz2"),
			},
		},
	} {
		// Set which limit tests should be performed for this query.
		testCaseLimitTests := test.limitTests
		if len(limitTests) == 0 {
			// Just test case where both are complete.
			testCaseLimitTests = []limitTest{bothCompleteLimitTest}
		}

		type testVariation struct {
			limitTest                    limitTest
			includeBothExpandableAndLeaf bool
			expectedResults              results
		}

		var testVarations []testVariation
		for _, limitTest := range testCaseLimitTests {
			testVarations = append(testVarations,
				// Test case with default find result options.
				testVariation{
					limitTest:                    limitTest,
					includeBothExpandableAndLeaf: false,
					expectedResults:              test.expectedResultsWithoutExpandableAndLeafDuplicates,
				},
				// Test case test for overloaded find result options.
				testVariation{
					limitTest:                    limitTest,
					includeBothExpandableAndLeaf: true,
					expectedResults:              test.expectedResultsWithExpandableAndLeafDuplicates,
				})
		}

		for _, variation := range testVarations {
			// nolint: govet
			limitTest := variation.limitTest
			includeBothExpandableAndLeaf := variation.includeBothExpandableAndLeaf
			expectedResults := variation.expectedResults
			t.Run(fmt.Sprintf("%s-%s", test.query, limitTest.name), func(t *testing.T) {
				ctrl := xtest.NewController(t)
				defer ctrl.Finish()

				store := storage.NewMockStorage(ctrl)

				if q := test.terminatedQuery; q != nil {
					// Set up no children case.
					store.EXPECT().
						CompleteTags(gomock.Any(), q.expectMatchers, gomock.Any()).
						Return(q.mockResult(limitTest), nil)
				}

				if q := test.childQuery; q != nil {
					// Set up children case.
					store.EXPECT().
						CompleteTags(gomock.Any(), q.expectMatchers, gomock.Any()).
						Return(q.mockResult(limitTest), nil)
				}

				builder, err := handleroptions.NewFetchOptionsBuilder(
					handleroptions.FetchOptionsBuilderOptions{
						Timeout: 15 * time.Second,
					})
				require.NoError(t, err)

				handlerOpts := options.EmptyHandlerOptions().
					SetGraphiteFindFetchOptionsBuilder(builder).
					SetStorage(store)
				// Set the relevant result options and save back to handler options.
				graphiteStorageOpts := handlerOpts.GraphiteStorageOptions()
				graphiteStorageOpts.FindResultsIncludeBothExpandableAndLeaf = includeBothExpandableAndLeaf
				handlerOpts = handlerOpts.SetGraphiteStorageOptions(graphiteStorageOpts)

				h := NewFindHandler(handlerOpts)

				// Execute the query.
				params := make(url.Values)
				params.Set("query", test.query)
				params.Set("from", from.s)
				params.Set("until", until.s)

				w := &writer{}
				req := &http.Request{Method: opts.httpMethod}
				switch opts.httpMethod {
				case http.MethodGet:
					req.URL = &url.URL{
						RawQuery: params.Encode(),
					}
				case http.MethodPost:
					req.Form = params
				}

				h.ServeHTTP(w, req)

				// Convert results to comparable format.
				require.Equal(t, 1, len(w.results))
				r := make(results, 0)
				decoder := json.NewDecoder(bytes.NewBufferString((w.results[0])))
				require.NoError(t, decoder.Decode(&r))
				sort.Sort(r)

				require.Equal(t, expectedResults, r)
				actual := w.Header().Get(headers.LimitHeader)
				assert.Equal(t, limitTest.header, actual)
			})
		}
	}
}
