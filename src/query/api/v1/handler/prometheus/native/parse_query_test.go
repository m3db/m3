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
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	pql "github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var parseTests = []struct {
	query string
	ex    string
}{
	{
		"foo",
		`{"name":"fetch"}`,
	},
	{
		"sum(a)-3",
		`{
			"name": "-",
			"children": [
				{
					"name": "sum",
					"children": [
						{
							"name": "fetch"
						}
					]
				},
				{
					"name": "scalar"
				}
			]
		}`,
	},
	{
		"1 > bool (foo or sum(rate(bar[5m])))",
		`{
			"children": [
				{
					"name": "scalar"
				},
				{
					"children": [
						{
							"name": "fetch"
						},
						{
							"children": [
								{
									"children": [
										{
											"name": "fetch"
										}
									],
									"name": "rate"
								}
							],
							"name": "sum"
						}
					],
					"name": "or"
				}
			],
			"name": ">"
		}`,
	},
}

func TestParse(t *testing.T) {
	opts := executor.NewEngineOptions().
		SetInstrumentOptions(instrument.NewOptions())

	count := 0
	parse := opts.ParseOptions().ParseFn()
	opts = opts.SetParseOptions(
		opts.ParseOptions().SetParseFn(
			func(query string) (pql.Expr, error) {
				count++
				return parse(query)
			},
		),
	)

	engine := executor.NewEngine(opts)
	for i, tt := range parseTests {
		handlerOpts := options.EmptyHandlerOptions().SetEngine(engine)
		h := NewPromParseHandler(handlerOpts)
		query := fmt.Sprintf("/parse?query=%s", url.QueryEscape(tt.query))
		req := httptest.NewRequest("GET", query, nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)
		body := w.Result().Body
		defer body.Close()

		r, err := ioutil.ReadAll(body)
		require.NoError(t, err)

		ex := xtest.MustPrettyJSONString(t, tt.ex)
		actual := xtest.MustPrettyJSONString(t, string(r))
		require.Equal(t, ex, actual,
			fmt.Sprintf("Run %d:\n%s", i, xtest.Diff(ex, actual)))
	}

	// Assure custom parser has been called for each of these queries.
	assert.Equal(t, len(parseTests), count)
}
