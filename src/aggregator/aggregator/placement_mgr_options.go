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
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInstanceID = "localhost"
)

// PlacementManagerOptions provide a set of options for the placement manager.
type PlacementManagerOptions interface {
	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) PlacementManagerOptions

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) PlacementManagerOptions

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetInstanceID sets the instance id.
	SetInstanceID(value string) PlacementManagerOptions

	// InstanceID returns the instance id.
	InstanceID() string

	// SetStagedPlacementWatcher sets the staged placement watcher.
	SetStagedPlacementWatcher(value placement.StagedPlacementWatcher) PlacementManagerOptions

	// StagedPlacementWatcher returns the staged placement watcher.
	StagedPlacementWatcher() placement.StagedPlacementWatcher
}

type placementManagerOptions struct {
	clockOpts        clock.Options
	instrumentOpts   instrument.Options
	instanceID       string
	placementWatcher placement.StagedPlacementWatcher
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

func (o *placementManagerOptions) SetStagedPlacementWatcher(value placement.StagedPlacementWatcher) PlacementManagerOptions {
	opts := *o
	opts.placementWatcher = value
	return &opts
}

func (o *placementManagerOptions) StagedPlacementWatcher() placement.StagedPlacementWatcher {
	return o.placementWatcher
}
