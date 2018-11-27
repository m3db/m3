// Copyright (c) 2018 Uber Technologies, Inc.
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

package prometheus

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/assert"
)

func TestPromCompressedReadSuccess(t *testing.T) {
	req, _ := http.NewRequest("POST", "dummy", test.GeneratePromReadBody(t))
	_, err := ParsePromCompressedRequest(req)
	assert.NoError(t, err)
}

func TestPromCompressedReadNoBody(t *testing.T) {
	req, _ := http.NewRequest("POST", "dummy", nil)
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestPromCompressedReadEmptyBody(t *testing.T) {
	req, _ := http.NewRequest("POST", "dummy", bytes.NewReader([]byte{}))
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestPromCompressedReadInvalidEncoding(t *testing.T) {
	req, _ := http.NewRequest("POST", "dummy", bytes.NewReader([]byte{'a'}))
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestTimeoutParse(t *testing.T) {
	req, _ := http.NewRequest("POST", "dummy", nil)
	req.Header.Add("timeout", "1ms")

	timeout, err := ParseRequestTimeout(req)
	assert.NoError(t, err)
	assert.Equal(t, timeout, time.Millisecond)

	req.Header.Del("timeout")
	timeout, err = ParseRequestTimeout(req)
	assert.NoError(t, err)
	assert.Equal(t, timeout, defaultTimeout)

	req.Header.Add("timeout", "invalid")
	_, err = ParseRequestTimeout(req)
	assert.Error(t, err)
}

type writer struct {
	value string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.value = string(p)
	return len(p), nil
}

func makeResult() []*storage.CompleteTagsResult {
	return []*storage.CompleteTagsResult{
		&storage.CompleteTagsResult{
			CompletedTags: []storage.CompletedTag{
				storage.CompletedTag{
					Name:   []byte("a"),
					Values: [][]byte{[]byte("1"), []byte("2"), []byte("3")},
				},
				storage.CompletedTag{
					Name:   []byte("b"),
					Values: [][]byte{[]byte("1"), []byte("2")},
				},
				storage.CompletedTag{
					Name:   []byte("c"),
					Values: [][]byte{[]byte("1"), []byte("2"), []byte("3")},
				},
			},
		},
	}
}

func TestRenderSeriesMatchResults(t *testing.T) {
	w := &writer{value: ""}
	seriesMatchResult := makeResult()

	expectedWhitespace := `{
		"status":"success",
		"data":[
			{"a":"1","b":"1","c":"1"},
			{"a":"1","b":"1","c":"2"},
			{"a":"1","b":"1","c":"3"},
			{"a":"1","b":"2","c":"1"},
			{"a":"1","b":"2","c":"2"},
			{"a":"1","b":"2","c":"3"},
			{"a":"2","b":"1","c":"1"},
			{"a":"2","b":"1","c":"2"},
			{"a":"2","b":"1","c":"3"},
			{"a":"2","b":"2","c":"1"},
			{"a":"2","b":"2","c":"2"},
			{"a":"2","b":"2","c":"3"},
			{"a":"3","b":"1","c":"1"},
			{"a":"3","b":"1","c":"2"},
			{"a":"3","b":"1","c":"3"},
			{"a":"3","b":"2","c":"1"},
			{"a":"3","b":"2","c":"2"},
			{"a":"3","b":"2","c":"3"}
		]
	}`

	err := RenderSeriesMatchResultsJSON(w, seriesMatchResult)
	assert.NoError(t, err)
	fields := strings.Fields(expectedWhitespace)
	expected := ""
	for _, field := range fields {
		expected = expected + field
	}

	assert.Equal(t, expected, w.value)
}
