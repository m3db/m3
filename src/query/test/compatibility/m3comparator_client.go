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
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type m3comparatorClient struct {
	host string
	port int
}

func newM3ComparatorClient(host string, port int) *m3comparatorClient {
	return &m3comparatorClient{
		host: host,
		port: port,
	}
}

func (c *m3comparatorClient) clear() error {
	comparatorURL := fmt.Sprintf("http://%s:%d", c.host, c.port)
	req, err := http.NewRequest(http.MethodDelete, comparatorURL, nil)
	if err != nil {
		return err
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (c *m3comparatorClient) load(data []byte) error {
	comparatorURL := fmt.Sprintf("http://%s:%d", c.host, c.port)
	resp, err := http.Post(comparatorURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("got error loading data to comparator %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/200 == 1 {
		return nil
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("load status code %d. Error: %v", resp.StatusCode, err)
	}

	return fmt.Errorf("load status code %d. Response: %s", resp.StatusCode, string(bodyBytes))
}
