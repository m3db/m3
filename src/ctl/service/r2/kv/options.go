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

package kv

import (
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultRuleUpdatePropagationDelay = time.Minute
)

// StoreOptions is a set of options for a kv backed store.
type StoreOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) StoreOptions

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) StoreOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetRuleUpdatePropagationDelay sets the propagation delay for rule updates.
	SetRuleUpdatePropagationDelay(value time.Duration) StoreOptions

	// RuleUpdatePropagationDelay returns the propagation delay for rule updates.
	RuleUpdatePropagationDelay() time.Duration
}

type storeOptions struct {
	clockOpts                  clock.Options
	instrumentOpts             instrument.Options
	ruleUpdatePropagationDelay time.Duration
}

// NewStoreOptions creates a new set of store options.
func NewStoreOptions() StoreOptions {
	return &storeOptions{
		clockOpts:                  clock.NewOptions(),
		instrumentOpts:             instrument.NewOptions(),
		ruleUpdatePropagationDelay: defaultRuleUpdatePropagationDelay,
	}
}

func (o *storeOptions) SetClockOptions(value clock.Options) StoreOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *storeOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *storeOptions) SetInstrumentOptions(value instrument.Options) StoreOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *storeOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *storeOptions) SetRuleUpdatePropagationDelay(value time.Duration) StoreOptions {
	opts := *o
	opts.ruleUpdatePropagationDelay = value
	return &opts
}

func (o *storeOptions) RuleUpdatePropagationDelay() time.Duration {
	return o.ruleUpdatePropagationDelay
}
