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

package execution

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Request is input for parallel execution
type Request interface {
	Process(ctx context.Context) error
}

// RequestResponse is used to combine both request and response in output
type RequestResponse struct {
	Request  Request
	Response *Response
}

// Response is returned from parallel execution
type Response struct {
	Value interface{}
	Err   error
}

// ExecuteParallel executes a slice of requests in parallel
func ExecuteParallel(ctx context.Context, requests []Request) error {
	return processParallel(ctx, requests)
}

// Process the requests in parallel and stop on first error
func processParallel(ctx context.Context, requests []Request) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, req := range requests {
		// Need to use a separate func since g.Go doesn't take input
		req := req
		func() {
			g.Go(func() error {
				return req.Process(ctx)
			})
		}()
	}

	return g.Wait()
}
