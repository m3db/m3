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

package client

import (
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

const timeout = time.Duration(5 * time.Second)

// DoGet is the low level call to the backend api for gets.
func DoGet(
	url string,
	headers map[string]string,
	l *zap.Logger,
) ([]byte, error) {
	l.Info("request", zap.String("method", "get"), zap.String("url", url))
	client := http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	setHeadersWithDefaults(req, headers)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, l); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

// DoPost is the low level call to the backend api for posts.
func DoPost(
	url string,
	headers map[string]string,
	data io.Reader,
	l *zap.Logger,
) ([]byte, error) {
	l.Info("request", zap.String("method", "post"), zap.String("url", url))
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodPost, url, data)
	if err != nil {
		return nil, err
	}

	setHeadersWithDefaults(req, headers)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, l); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

// DoDelete is the low level call to the backend api for deletes.
func DoDelete(
	url string,
	headers map[string]string,
	l *zap.Logger,
) ([]byte, error) {
	l.Info("request", zap.String("method", "delete"), zap.String("url", url))
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	setHeadersWithDefaults(req, headers)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, l); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

func setHeadersWithDefaults(req *http.Request, headers map[string]string) {
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}
