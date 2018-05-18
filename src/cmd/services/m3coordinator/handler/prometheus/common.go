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
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"

	"github.com/golang/snappy"
)

const (
	maxTimeout     = time.Minute
	defaultTimeout = time.Second * 15
)

// RequestParams are the arguments to a call
type RequestParams struct {
	Timeout time.Duration
}

// ParsePromCompressedRequest parses a snappy compressed request from Prometheus
func ParsePromCompressedRequest(r *http.Request) ([]byte, *handler.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}
	defer body.Close()
	compressed, err := ioutil.ReadAll(body)

	if err != nil {
		return nil, handler.NewParseError(err, http.StatusInternalServerError)
	}

	if len(compressed) == 0 {
		return nil, handler.NewParseError(fmt.Errorf("empty request body"), http.StatusBadRequest)
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return reqBuf, nil
}

// ParseRequestParams parses the input request parameters and provides useful defaults
func ParseRequestParams(r *http.Request) (*RequestParams, error) {
	var params RequestParams
	timeout := r.Header.Get("timeout")
	if timeout != "" {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid 'timeout': %v", handler.ErrInvalidParams, err)
		}

		if duration > maxTimeout {
			return nil, fmt.Errorf("%s: invalid 'timeout': greater than %v", handler.ErrInvalidParams, maxTimeout)
		}

		params.Timeout = duration
	} else {
		params.Timeout = defaultTimeout
	}

	return &params, nil
}
