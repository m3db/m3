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

package router

import (
	"github.com/m3db/m3/src/aggregator/aggregator/handler/common"
	"github.com/m3db/m3x/errors"
)

type broadcastRouter struct {
	routers []Router
}

// NewBroadcastRouter creates a broadcast router.
func NewBroadcastRouter(routers []Router) Router {
	return &broadcastRouter{routers: routers}
}

func (r *broadcastRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	multiErr := errors.NewMultiError()
	for _, router := range r.routers {
		buffer.IncRef()
		if err := router.Route(shard, buffer); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	buffer.DecRef()
	return multiErr.FinalError()
}

func (r *broadcastRouter) Close() {
	for _, router := range r.routers {
		router.Close()
	}
}
