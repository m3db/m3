// Copyright (c) 2017 Uber Technologies, Inc.
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

package backend

import "github.com/m3db/m3metrics/policy"

// Server is a backend server that is responsible for encoding the
// incoming metrics and the corresponding policies, and routing them to
// the downstream service.
type Server interface {
	// Open opens the server.
	Open() error

	// WriteCounterWithPolicies writes a counter along with policies.
	WriteCounterWithPolicies(id []byte, val int64, vp policy.VersionedPolicies) error

	// WriteBatchTimerWithPolicies writes a batch timer along with policies.
	WriteBatchTimerWithPolicies(id []byte, val []float64, vp policy.VersionedPolicies) error

	// WriteGaugeWithPolicies writes a gauge along with policies.
	WriteGaugeWithPolicies(id []byte, val float64, vp policy.VersionedPolicies) error

	// Flush flushes any remaining data buffered by the server.
	Flush() error

	// Close closes the server.
	Close() error
}
