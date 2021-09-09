// Copyright (c) 2021  Uber Technologies, Inc.
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

import "context"

// NewContextWithEndpoint creates a new context.Context with the Endpoint set
// as a value.
func NewContextWithEndpoint(ctx context.Context, endpoint Endpoint) context.Context {
	return context.WithValue(ctx, EndpointContextKey, endpoint)
}

// EndpointFromContext returns the Endpoint within the context
// or Unknown if not available.
func EndpointFromContext(ctx context.Context) Endpoint {
	var (
		endpoint Endpoint
		ok       bool
	)
	val := ctx.Value(EndpointContextKey)
	if val != nil {
		endpoint, ok = val.(Endpoint)
		if !ok {
			endpoint = Unknown
		}
	}

	return endpoint
}

// String returns the string value of Endpoint enum.
func (e Endpoint) String() string {
	switch e {
	case AggregateRaw:
		return "AggregateRaw"
	case Fetch:
		return "Fetch"
	case FetchBatchRaw:
		return "FetchBatchRaw"
	case FetchBatchRawV2:
		return "FetchBatchRawV2"
	case FetchTagged:
		return "FetchTagged"
	case Query:
		return "Query"
	case Unknown:
		fallthrough
	default:
		return "Unknown"
	}
}
