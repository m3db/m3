// Copyright (c) 2016 Uber Technologies, Inc.
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

package tchannelthrift

import (
	stdctx "context"
	"time"

	"github.com/m3db/m3/src/x/context"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const (
	contextKey = "m3dbcontext"
)

// RegisterServer will register a tchannel thrift server and create and close M3DB contexts per request
func RegisterServer(channel *tchannel.Channel, service thrift.TChanServer, contextPool context.Pool) {
	server := thrift.NewServer(channel)
	server.Register(service, thrift.OptPostResponse(postResponseFn))
	server.SetContextFn(func(ctx stdctx.Context, method string, headers map[string]string) thrift.Context {
		xCtx := contextPool.Get()
		xCtx.SetGoContext(ctx)
		ctxWithValue := stdctx.WithValue(ctx, contextKey, xCtx) //nolint: staticcheck
		return thrift.WithHeaders(ctxWithValue, headers)
	})
}

// NewContext returns a new thrift context and cancel func with embedded M3DB context
func NewContext(timeout time.Duration) (thrift.Context, stdctx.CancelFunc) {
	tctx, cancel := thrift.NewContext(timeout)
	xCtx := context.NewWithGoContext(tctx)
	ctxWithValue := stdctx.WithValue(tctx, contextKey, xCtx) //nolint: staticcheck
	return thrift.WithHeaders(ctxWithValue, nil), cancel
}

// Context returns an M3DB context from the thrift context
func Context(ctx thrift.Context) context.Context {
	return ctx.Value(contextKey).(context.Context)
}

func postResponseFn(ctx stdctx.Context, method string, response apachethrift.TStruct) {
	value := ctx.Value(contextKey)
	inner := value.(context.Context)
	inner.Close()
}
