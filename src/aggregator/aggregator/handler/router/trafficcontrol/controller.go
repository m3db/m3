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

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util"
	"github.com/m3db/m3x/watch"

	"go.uber.org/atomic"
)

const (
	defaultInitTimeout = 2 * time.Second
)

// Controller controls traffic.
type Controller interface {
	// Allow returns true if traffic is allowed.
	Allow() bool

	// Init initializes the traffic controller to watch the runtime updates.
	Init() error

	// Close closes the traffic controller.
	Close()
}

type trafficEnabler struct {
	enabled *atomic.Bool
	value   watch.Value
	opts    Options
}

// NewTrafficEnabler creates a new traffic enabler.
func NewTrafficEnabler(opts Options) Controller {
	enabled := atomic.NewBool(opts.DefaultValue())
	iOpts := opts.InstrumentOptions()
	newUpdatableFn := func() (watch.Updatable, error) {
		w, err := opts.Store().Watch(opts.RuntimeKey())
		return w, err
	}
	getFn := func(updatable watch.Updatable) (interface{}, error) {
		return updatable.(kv.ValueWatch).Get(), nil
	}
	processFn := func(update interface{}) error {
		b, err := util.BoolFromValue(
			update.(kv.Value),
			opts.RuntimeKey(),
			opts.DefaultValue(),
			util.NewOptions().SetLogger(iOpts.Logger()),
		)
		if err != nil {
			return err
		}
		enabled.Store(b)
		return nil
	}
	vOptions := watch.NewOptions().
		SetInitWatchTimeout(opts.InitTimeout()).
		SetInstrumentOptions(iOpts).
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getFn).
		SetProcessFn(processFn)
	return &trafficEnabler{
		enabled: enabled,
		value:   watch.NewValue(vOptions),
		opts:    opts,
	}
}

func (c *trafficEnabler) Init() error {
	err := c.value.Watch()
	if err == nil {
		return nil
	}
	if _, ok := err.(watch.CreateWatchError); ok {
		return err
	}
	return nil
}

func (c *trafficEnabler) Close() {
	c.value.Unwatch()
}

func (c *trafficEnabler) Allow() bool {
	return c.enabled.Load()
}

type trafficDisabler struct {
	Controller
}

// NewTrafficDisabler creates a new traffic disabler.
func NewTrafficDisabler(opts Options) Controller {
	return &trafficDisabler{
		Controller: NewTrafficEnabler(opts),
	}
}

func (c *trafficDisabler) Allow() bool {
	return !c.Controller.Allow()
}
