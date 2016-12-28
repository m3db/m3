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

package server

import (
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// Options provide a set of server options
type Options interface {
	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetRetrier sets the retrier for accepting connections
	SetRetrier(value xretry.Retrier) Options

	// Retrier returns the retrier for accepting connections
	Retrier() xretry.Retrier

	// SetIteratorPool sets the iterator pool
	SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options

	// IteratorPool returns the iterator pool
	IteratorPool() msgpack.UnaggregatedIteratorPool

	// SetPacketQueueSize sets the packet queue size
	SetPacketQueueSize(value int) Options

	// PacketQueueSize returns the packet queue size
	PacketQueueSize() int

	// SetWorkerPoolSize sets the worker pool size
	SetWorkerPoolSize(value int) Options

	// WorkerPoolSize returns the worker pool size
	WorkerPoolSize() int
}
