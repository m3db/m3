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

package handler

import (
	"context"
	"net/http"

	"github.com/m3db/m3/src/query/util/logging"
)

// CloseWatcher watches for CloseNotify and context timeout. It is best effort and may sometimes not close the channel relying on gc
func CloseWatcher(ctx context.Context, w http.ResponseWriter) (<-chan bool, <-chan bool) {
	closing := make(chan bool)
	logger := logging.WithContext(ctx)
	var doneChan <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		done := make(chan bool)

		notify := notifier.CloseNotify()
		go func() {
			// Wait for either the request to finish
			// or for the client to disconnect
			select {
			case <-done:
			case <-notify:
				logger.Warn("connection closed by client")
				close(closing)
			case <-ctx.Done():
				// We only care about the time out case and not other cancellations
				if ctx.Err() == context.DeadlineExceeded {
					logger.Warn("request timed out")
				}
				close(closing)
			}
			close(done)
		}()
		doneChan = done
	}

	return doneChan, closing
}
