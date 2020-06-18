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

package compatibility

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type m3queryClient struct {
	host string
	port int
}

func newM3QueryClient(host string, port int) *m3queryClient {
	return &m3queryClient{
		host: host,
		port: port,
	}
}

func (c *m3queryClient) query(expr string, t time.Time) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/m3query/api/v1/query", c.host, c.port)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("query", expr)
	q.Add("time", fmt.Sprint(t.Unix()))
	req.URL.RawQuery = q.Encode()
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, errors.Wrapf(err, "error evaluating query %s", expr)
	}

	defer resp.Body.Close()
	if resp.StatusCode/200 != 1 {
		return nil, fmt.Errorf("invalid status %+v received sending query: %+v", resp.StatusCode, req)
	}

	return ioutil.ReadAll(resp.Body)
}
