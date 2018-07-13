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
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/test"

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
