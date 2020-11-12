// Copyright (c) 2020 Uber Technologies, Inc.
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

package harness

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/cmd/tools/dtest/docker/harness/resources"
)

type urlTest struct {
	name string
	url  string
}

func TestInvalidInstantQueryReturns400(t *testing.T) {
	coord := singleDBNodeDockerResources.Coordinator()

	instantQueryBadRequestTest := []urlTest{
		// FAILING issue #2: invalid or missing query string should result in 400
		// {"missing query", queryURL("query", "")},
		// {"invalid query", queryURL("query", "@!")},
		{"invalid time", queryURL("time", "INVALID")},
		{"invalid timeout", queryURL("timeout", "INVALID")},
	}

	for _, tt := range instantQueryBadRequestTest {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, coord.RunQuery(verifyStatus(400), tt.url), "for query '%v'", tt.url)
		})
	}
}

func TestInvalidRangeQueryReturns400(t *testing.T) {
	coord := singleDBNodeDockerResources.Coordinator()

	queryBadRequestTest := []urlTest{
		{"missing query", queryRangeURL("query", "")},
		// FAILING issue #2: invalid query string should result in 400
		// {"invalid query", queryRangeURL("query", "@!")},
		{"missing start", queryRangeURL("start", "")},
		{"invalid start", queryRangeURL("start", "INVALID")},
		{"missing end", queryRangeURL("end", "")},
		{"invalid end", queryRangeURL("end", "INVALID")},
		{"missing step", queryRangeURL("step", "")},
		{"invalid step", queryRangeURL("step", "INVALID")},
		{"invalid timeout", queryRangeURL("timeout", "INVALID")},
	}

	for _, tt := range queryBadRequestTest {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, coord.RunQuery(verifyStatus(400), tt.url), "for query '%v'", tt.url)
		})
	}
}

func queryURL(key, value string) string {
	params := map[string]string{
		"query": "foo",
	}

	if value == "" {
		delete(params, key)
	} else {
		params[key] = value
	}

	return "api/v1/query?" + queryString(params)
}

func queryRangeURL(key, value string) string {
	params := map[string]string{
		"query": "foo",
		"start": "now",
		"end":   "now",
		"step":  "1000",
	}

	if value == "" {
		delete(params, key)
	} else {
		params[key] = value
	}

	return "api/v1/query_range?" + queryString(params)
}

func queryString(params map[string]string) string {
	p := make([]string, 0)
	for k, v := range params {
		p = append(p, fmt.Sprintf("%v=%v", k, v))
	}

	return strings.Join(p, "&")
}

func verifyStatus(expectedStatus int) resources.ResponseVerifier {
	return func(status int, resp string, err error) error {
		if err != nil {
			return err
		}

		if status != expectedStatus {
			return fmt.Errorf("expeceted %v status code, got %v", expectedStatus, status)
		}

		return nil
	}
}
