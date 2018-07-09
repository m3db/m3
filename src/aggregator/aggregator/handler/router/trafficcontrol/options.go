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

package trafficcontrol

import (
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/instrument"
)

// Options configurates the traffic controller.
type Options interface {
	// SetStore sets the kv store.
	SetStore(store kv.Store) Options

	// Store returns the kv store.
	Store() kv.Store

	// SetDefaultValue sets the default value.
	SetDefaultValue(value bool) Options

	// DefaultValue returns the default value.
	DefaultValue() bool

	// SetRuntimeKey sets the runtime enable key,
	// which will override the default enabled value when present.
	SetRuntimeKey(value string) Options

	// RuntimeKey returns the runtime enable key,
	// which will override the default enabled value when present.
	RuntimeKey() string

	// SetInitTimeout sets the init timeout.
	SetInitTimeout(value time.Duration) Options

	// InitTimeout returns the init timeout.
	InitTimeout() time.Duration

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options
}

type trafficControlOptions struct {
	store          kv.Store
	defaultValue   bool
	runtimeKey     string
	initTimeout    time.Duration
	instrumentOpts instrument.Options
}

// NewOptions creates new Options.
func NewOptions() Options {
	return &trafficControlOptions{
		initTimeout:    defaultInitTimeout,
		defaultValue:   false,
		instrumentOpts: instrument.NewOptions(),
	}
}

func (o *trafficControlOptions) SetStore(store kv.Store) Options {
	opts := *o
	opts.store = store
	return &opts
}

func (o *trafficControlOptions) Store() kv.Store {
	return o.store
}

func (o *trafficControlOptions) SetDefaultValue(value bool) Options {
	opts := *o
	opts.defaultValue = value
	return &opts
}

func (o *trafficControlOptions) DefaultValue() bool {
	return o.defaultValue
}

func (o *trafficControlOptions) SetRuntimeKey(value string) Options {
	opts := *o
	opts.runtimeKey = value
	return &opts
}

func (o *trafficControlOptions) RuntimeKey() string {
	return o.runtimeKey
}

func (o *trafficControlOptions) SetInitTimeout(value time.Duration) Options {
	opts := *o
	opts.initTimeout = value
	return &opts
}

func (o *trafficControlOptions) InitTimeout() time.Duration {
	return o.initTimeout
}

func (o *trafficControlOptions) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *trafficControlOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}
