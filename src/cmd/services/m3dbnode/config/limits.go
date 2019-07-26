// Copyright (c) 2019 Uber Technologies, Inc.
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

package config

// Limits contains configuration for configurable limits that can be applied to M3DB.
type Limits struct {
	// MaxOutstandingWriteRequests controls the maximum number of outstanding write RPCs
	// that the server will allow before it begins rejecting requests. Note that this value
	// is independent of the number of values that are being written (due to variable batch
	// from the client) but is still very useful for enforcing backpressure due to the fact
	// that all writes within a single RPC are single-threaded.
	MaxOutstandingWriteRequests int `yaml:"maxOutstandingWriteRequests" validate:"min=0"`
	// MaxOutstandingReadRequests controls the maximum number of outstanding read RPCs that
	// the server will allow before it begins rejecting requests. Just like MaxOutstandingWriteRequests
	// this value is independent of the number of time series being read.
	MaxOutstandingReadRequests int `yaml:"maxOutstandingReadRequests" validate:"min=0"`
}
