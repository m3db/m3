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

package etcd

import (
	"io"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/x/instrument"
)

// EmbeddedKV is an embedded etcd server wrapped around
// by m3cluster utilities.
type EmbeddedKV interface {
	io.Closer

	// Start starts the embedded KV.
	Start() error

	// Endpoints returns the active endpoints for the embedded KV.
	Endpoints() []string

	// ConfigServiceClient returns a m3cluster wrapper
	// around the embedded KV.
	ConfigServiceClient(fns ...ClientOptFn) (client.Client, error)
}

// ClientOptFn updates etcdclient.Option with any custom configs.
type ClientOptFn func(etcdclient.Options) etcdclient.Options

// Options specify the knobs to control the embedded KV.
type Options interface {
	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options

	// SetDir sets the working directory.
	SetDir(value string) Options

	// Dir returns the working directory.
	Dir() string

	// SetInitTimeout sets the init timeout.
	SetInitTimeout(value time.Duration) Options

	// InitTimeout returns the init timeout.
	InitTimeout() time.Duration

	// SetServiceID sets the service id for KV operations.
	SetServiceID(value string) Options

	// ServiceID returns the service id for KV operations.
	ServiceID() string

	// SetEnvironment sets the environment for KV operations.
	SetEnvironment(value string) Options

	// Environment returns the environment for KV operations.
	Environment() string

	// SetZone sets the zone for KV operations.
	SetZone(value string) Options

	// Zone returns the zone for KV operations.
	Zone() string
}
