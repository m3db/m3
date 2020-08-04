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
	"github.com/m3db/m3/src/x/instrument"
)

// CancelWatcher is an interface that wraps a WatchForCancel method.
// TODO: make this generic middleware, rather than applied per function.
type CancelWatcher interface {
	// WatchForCancel watches on the given context, and applies
	// the given cancellation function.
	WatchForCancel(context.Context, context.CancelFunc)
}

type canceller struct {
	notifier http.CloseNotifier
	iOpts    instrument.Options
}

func (c *canceller) WatchForCancel(
	ctx context.Context,
	cancel context.CancelFunc,
) {
	logger := logging.WithContext(ctx, c.iOpts)
	notify := c.notifier.CloseNotify()
	go func() {
		// Wait for either the request to finish
		// or for the client to disconnect
		select {
		case <-notify:
			logger.Warn("connection closed by client")
			cancel()
		case <-ctx.Done():
			// We only care about the time out case and not other cancellations
			if ctx.Err() == context.DeadlineExceeded {
				logger.Warn("request timed out")
			}
		}
	}()
}

type ctxCanceller struct {
	iOpts instrument.Options
}

func (c *ctxCanceller) WatchForCancel(
	ctx context.Context, _ context.CancelFunc,
) {
	logger := logging.WithContext(ctx, c.iOpts)
	go func() {
		select {
		case <-ctx.Done():
			// We only care about the time out case and not other cancellations
			if ctx.Err() == context.DeadlineExceeded {
				logger.Warn("request timed out")
			}
		}
	}()
}

// NewResponseWriterCanceller creates a canceller on the given context with
// the given response writer.
func NewResponseWriterCanceller(
	w http.ResponseWriter,
	iOpts instrument.Options,
) CancelWatcher {
	notifier, ok := w.(http.CloseNotifier)
	if !ok {
		return &ctxCanceller{iOpts: iOpts}
	}

	return &canceller{notifier: notifier, iOpts: iOpts}
}

// CloseWatcher watches for CloseNotify and context timeout.
// It is best effort and may sometimes not close the channel relying on GC.
func CloseWatcher(
	ctx context.Context,
	cancel context.CancelFunc,
	w http.ResponseWriter,
	instrumentOpts instrument.Options,
) {
	notifier, ok := w.(http.CloseNotifier)
	if !ok {
		return
	}

	logger := logging.WithContext(ctx, instrumentOpts)
	notify := notifier.CloseNotify()
	go func() {
		// Wait for either the request to finish
		// or for the client to disconnect
		select {
		case <-notify:
			logger.Warn("connection closed by client")
			cancel()
		case <-ctx.Done():
			// We only care about the time out case and not other cancellations
			if ctx.Err() == context.DeadlineExceeded {
				logger.Warn("request timed out")
			}
		}
	}()
}
