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
	"io/ioutil"
	"net/http"

	"go.uber.org/zap"
)

func checkForAndHandleError(url string, resp *http.Response, zl *zap.Logger) error {
	if resp.StatusCode/100 != 2 {
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			zl.Error("error response",
				zap.Error(fmt.Errorf("status %d", resp.StatusCode)),
				zap.String("url", url),
				zap.ByteString("response", body))
		} else {
			zl.Error("error response",
				zap.Error(fmt.Errorf("status %d", resp.StatusCode)),
				zap.String("url", url),
				zap.Error(fmt.Errorf("response not available: %v", err)))
		}
		return fmt.Errorf("error response: status=%s, url=%s", resp.Status, url)
	}
	return nil
}
