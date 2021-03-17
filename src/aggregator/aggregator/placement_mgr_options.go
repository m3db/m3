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

package aggregator

import (
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultInstanceID = "localhost"
)

// PlacementManagerOptions provide a set of options for the placement manager.
type PlacementManagerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) PlacementManagerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) PlacementManagerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstanceID sets the instance id.
	SetInstanceID(value string) PlacementManagerOptions

	// InstanceID returns the instance id.
	InstanceID() string

	// SetWatcherOptions sets the placement watcher options.
	SetWatcherOptions(value placement.WatcherOptions) PlacementManagerOptions

	// WatcherOptions returns the placement watcher options.
	WatcherOptions() placement.WatcherOptions
}

type placementManagerOptions struct {
	clockOpts            clock.Options
	instrumentOpts       instrument.Options
	instanceID           string
	placementWatcherOpts placement.WatcherOptions
}

// NewPlacementManagerOptions creates a new set of placement manager options.
func NewPlacementManagerOptions() PlacementManagerOptions {
	return &placementManagerOptions{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		instanceID:     defaultInstanceID,
	}
}

func (o *placementManagerOptions) SetClockOptions(value clock.Options) PlacementManagerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *placementManagerOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *placementManagerOptions) SetInstrumentOptions(value instrument.Options) PlacementManagerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *placementManagerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *placementManagerOptions) SetInstanceID(value string) PlacementManagerOptions {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *placementManagerOptions) InstanceID() string {
	return o.instanceID
}

func (o *placementManagerOptions) SetWatcherOptions(value placement.WatcherOptions) PlacementManagerOptions {
	opts := *o
	opts.placementWatcherOpts = value
	return &opts
}

func (o *placementManagerOptions) WatcherOptions() placement.WatcherOptions {
	return o.placementWatcherOpts
}
