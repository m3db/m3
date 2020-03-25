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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

const timeout = time.Duration(5 * time.Second)

// DoGet is the low level call to the backend api for gets
func DoGet(url string, getter func(reader io.Reader, zl *zap.Logger) error, zl *zap.Logger) error {
	zl.Info("request", zap.String("method", "get"), zap.String("url", url))
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, zl); err != nil {
		return err
	}
	return getter(resp.Body, zl)
}

// DoPost is the low level call to the backend api for posts
func DoPost(url string, data io.Reader, getter func(reader io.Reader, zl *zap.Logger) error, zl *zap.Logger) error {
	zl.Info("request", zap.String("method", "post"), zap.String("url", url))
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodPost, url, data)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, zl); err != nil {
		return err
	}
	return getter(resp.Body, zl)
}

// DoDelete is the low level call to the backend api for deletes
func DoDelete(url string, getter func(reader io.Reader, zl *zap.Logger) error, zl *zap.Logger) error {
	zl.Info("request", zap.String("method", "delete"), zap.String("url", url))
	client := &http.Client{
		Timeout: timeout,
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if err := checkForAndHandleError(url, resp, zl); err != nil {
		return err
	}
	return getter(resp.Body, zl)
}

// Dumper is a simple printer for http responses
func Dumper(in io.Reader, zl *zap.Logger) error {
	dat, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	fmt.Println(string(dat))
	return nil
}
