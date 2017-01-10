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

package integration

import "time"

const (
	defaultServerStateChangeTimeout = 5 * time.Second
	defaultClientBatchSize          = 1440
	defaultClientConnectTimeout     = time.Second
	defaultWorkerPoolSize           = 4
)

type testOptions interface {
	// SetListenAddr sets the listen address
	SetListenAddr(value string) testOptions

	// ListenAddr returns the listen address
	ListenAddr() string

	// SetClientBatchSize sets the client-side batch size
	SetClientBatchSize(value int) testOptions

	// ClientBatchSize returns the client-side batch size
	ClientBatchSize() int

	// SetClientConnectTimeout sets the client-side connect timeout
	SetClientConnectTimeout(value time.Duration) testOptions

	// ClientConnectTimeout returns the client-side connect timeout
	ClientConnectTimeout() time.Duration

	// SetServerStateChangeTimeout sets the client connect timeout
	SetServerStateChangeTimeout(value time.Duration) testOptions

	// ServerStateChangeTimeout returns the client connect timeout
	ServerStateChangeTimeout() time.Duration

	// SetWorkerPoolSize sets the number of workers in the worker pool.
	SetWorkerPoolSize(value int) testOptions

	// WorkerPoolSize returns the number of workers in the worker pool.
	WorkerPoolSize() int
}

type options struct {
	listenAddr               string
	serverStateChangeTimeout time.Duration
	workerPoolSize           int
	clientBatchSize          int
	clientConnectTimeout     time.Duration
}

func newTestOptions() testOptions {
	return &options{
		serverStateChangeTimeout: defaultServerStateChangeTimeout,
		workerPoolSize:           defaultWorkerPoolSize,
		clientBatchSize:          defaultClientBatchSize,
		clientConnectTimeout:     defaultClientConnectTimeout,
	}
}

func (o *options) SetListenAddr(value string) testOptions {
	opts := *o
	opts.listenAddr = value
	return &opts
}

func (o *options) ListenAddr() string {
	return o.listenAddr
}

func (o *options) SetClientBatchSize(value int) testOptions {
	opts := *o
	opts.clientBatchSize = value
	return &opts
}

func (o *options) ClientBatchSize() int {
	return o.clientBatchSize
}

func (o *options) SetClientConnectTimeout(value time.Duration) testOptions {
	opts := *o
	opts.clientConnectTimeout = value
	return &opts
}

func (o *options) ClientConnectTimeout() time.Duration {
	return o.clientConnectTimeout
}

func (o *options) SetServerStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) SetWorkerPoolSize(value int) testOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *options) WorkerPoolSize() int {
	return o.workerPoolSize
}
