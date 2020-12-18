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
	"encoding/json"
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
	urlPrefixes := []string{"", "prometheus/", "m3query/"}

	urlTests := addPrefixes(urlPrefixes, []urlTest{
		{"missing query", queryURL("query", "")},
		{"invalid query", queryURL("query", "@!")},
		{"invalid time", queryURL("time", "INVALID")},
		{"invalid timeout", queryURL("timeout", "INVALID")},
	})

	testInvalidQueryReturns400(t, urlTests)
}

func TestInvalidRangeQueryReturns400(t *testing.T) {
	urlPrefixes := []string{"", "prometheus/", "m3query/"}

	urlTests := addPrefixes(urlPrefixes, []urlTest{
		{"missing query", queryRangeURL("query", "")},
		{"invalid query", queryRangeURL("query", "@!")},
		{"missing start", queryRangeURL("start", "")},
		{"invalid start", queryRangeURL("start", "INVALID")},
		{"missing end", queryRangeURL("end", "")},
		{"invalid end", queryRangeURL("end", "INVALID")},
		{"missing step", queryRangeURL("step", "")},
		{"invalid step", queryRangeURL("step", "INVALID")},
		{"invalid timeout", queryRangeURL("timeout", "INVALID")},
	})

	testInvalidQueryReturns400(t, urlTests)
}

func testInvalidQueryReturns400(t *testing.T, tests []urlTest) {
	coord := singleDBNodeDockerResources.Coordinator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, coord.RunQuery(verifyResponse(400), tt.url), "for query '%v'", tt.url)
		})
	}
}

func addPrefixes(prefixes []string, tests []urlTest) []urlTest {
	res := make([]urlTest, 0)
	for _, prefix := range prefixes {
		for _, t := range tests {
			res = append(res, urlTest{
				fmt.Sprintf("%v %v", prefix, t.name),
				fmt.Sprintf("%v%v", prefix, t.url),
			})
		}
	}
	return res
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

func verifyResponse(expectedStatus int) resources.ResponseVerifier {
	return func(status int, headers map[string][]string, resp string, err error) error {
		if err != nil {
			return err
		}

		if status != expectedStatus {
			return fmt.Errorf("expeceted %v status code, got %v", expectedStatus, status)
		}

		if contentType, ok := headers["Content-Type"]; !ok {
			return fmt.Errorf("missing Content-Type header")
		} else if len(contentType) != 1 || contentType[0] != "application/json" {
			return fmt.Errorf("expected json content type, got %v", contentType)
		}

		errorResponse := struct {
			Status string `json:"status,omitempty"`
			Error  string `json:"error,omitempty"`
		}{}

		err = json.Unmarshal([]byte(resp), &errorResponse)
		if err != nil {
			return fmt.Errorf("failed unmarshalling response: %w", err)
		}

		if errorResponse.Status != "error" {
			return fmt.Errorf("expected body to contain status 'error', got %v", errorResponse.Status)
		}

		if errorResponse.Error == "" {
			return fmt.Errorf("expected body to contain error message")
		}

		return nil
	}
}
