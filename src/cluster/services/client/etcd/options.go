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

package etcd

import (
	"errors"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitTimeout   = 5 * time.Second
	defaultGaugeInterval = 10 * time.Second
)

var (
	errNoKVGen            = errors.New("no KVGen function set")
	errNoHeartbeatGen     = errors.New("no HeartbeatGen function set")
	errNoLeaderGen        = errors.New("no LeaderGen function set")
	errInvalidInitTimeout = errors.New("non-positive init timeout for service watch")
)

// KVGen generates a kv store for a given zone
type KVGen func(zone string) (kv.Store, error)

// HeartbeatGen generates a heartbeat store for a given zone
type HeartbeatGen func(sid services.ServiceID) (services.HeartbeatService, error)

// LeaderGen generates a leader service instance for a given service.
type LeaderGen func(sid services.ServiceID, opts services.ElectionOptions) (services.LeaderService, error)

// Options are options for the client of Services
type Options interface {
	// InitTimeout is the max time to wait on a new service watch for a valid initial value
	// If the value is set to 0, then no wait will be done and the watch could return empty value
	InitTimeout() time.Duration

	// SetInitTimeout sets the InitTimeout
	SetInitTimeout(t time.Duration) Options

	// KVGen is the function to generate a kv store for a given zone
	KVGen() KVGen

	// SetKVGen sets the KVGen
	SetKVGen(gen KVGen) Options

	// HeartbeatGen is the function to generate a heartbeat store for a given zone
	HeartbeatGen() HeartbeatGen

	// SetHeartbeatGen sets the HeartbeatGen
	SetHeartbeatGen(gen HeartbeatGen) Options

	// LeaderGen is the function to generate a leader service instance for a
	// given service.
	LeaderGen() LeaderGen

	// SetLeaderGen sets the leader generation function.
	SetLeaderGen(gen LeaderGen) Options

	// InstrumentsOptions is the instrument options
	InstrumentsOptions() instrument.Options

	// SetInstrumentsOptions sets the InstrumentsOptions
	SetInstrumentsOptions(iopts instrument.Options) Options

	// NamespaceOptions is the custom namespaces.
	NamespaceOptions() services.NamespaceOptions

	// SetNamespaceOptions sets the NamespaceOptions.
	SetNamespaceOptions(opts services.NamespaceOptions) Options

	// Validate validates the Options
	Validate() error
}

type options struct {
	initTimeout time.Duration
	nOpts       services.NamespaceOptions
	kvGen       KVGen
	hbGen       HeartbeatGen
	ldGen       LeaderGen
	iopts       instrument.Options
}

// NewOptions creates an Option
func NewOptions() Options {
	return options{
		iopts:       instrument.NewOptions(),
		nOpts:       services.NewNamespaceOptions(),
		initTimeout: defaultInitTimeout,
	}
}

func (o options) Validate() error {
	if o.kvGen == nil {
		return errNoKVGen
	}

	if o.hbGen == nil {
		return errNoHeartbeatGen
	}

	if o.ldGen == nil {
		return errNoLeaderGen
	}

	if o.initTimeout <= 0 {
		return errInvalidInitTimeout
	}

	return nil
}

func (o options) InitTimeout() time.Duration {
	return o.initTimeout
}

func (o options) SetInitTimeout(t time.Duration) Options {
	o.initTimeout = t
	return o
}

func (o options) KVGen() KVGen {
	return o.kvGen
}

func (o options) SetKVGen(gen KVGen) Options {
	o.kvGen = gen
	return o
}

func (o options) HeartbeatGen() HeartbeatGen {
	return o.hbGen
}

func (o options) SetHeartbeatGen(gen HeartbeatGen) Options {
	o.hbGen = gen
	return o
}

func (o options) LeaderGen() LeaderGen {
	return o.ldGen
}

func (o options) SetLeaderGen(lg LeaderGen) Options {
	o.ldGen = lg
	return o
}

func (o options) InstrumentsOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentsOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}

func (o options) NamespaceOptions() services.NamespaceOptions {
	return o.nOpts
}

func (o options) SetNamespaceOptions(opts services.NamespaceOptions) Options {
	o.nOpts = opts
	return o
}
