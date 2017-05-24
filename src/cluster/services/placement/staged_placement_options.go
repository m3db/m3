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

package placement

import (
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/clock"
)

type activeStagedPlacementOptions struct {
	clockOpts             clock.Options
	onPlacementsAddedFn   services.OnPlacementsAddedFn
	onPlacementsRemovedFn services.OnPlacementsRemovedFn
}

// NewActiveStagedPlacementOptions create a new set of active staged placement options.
func NewActiveStagedPlacementOptions() services.ActiveStagedPlacementOptions {
	return &activeStagedPlacementOptions{
		clockOpts: clock.NewOptions(),
	}
}

func (o *activeStagedPlacementOptions) SetClockOptions(value clock.Options) services.ActiveStagedPlacementOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *activeStagedPlacementOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *activeStagedPlacementOptions) SetOnPlacementsAddedFn(value services.OnPlacementsAddedFn) services.ActiveStagedPlacementOptions {
	opts := *o
	opts.onPlacementsAddedFn = value
	return &opts
}

func (o *activeStagedPlacementOptions) OnPlacementsAddedFn() services.OnPlacementsAddedFn {
	return o.onPlacementsAddedFn
}

func (o *activeStagedPlacementOptions) SetOnPlacementsRemovedFn(value services.OnPlacementsRemovedFn) services.ActiveStagedPlacementOptions {
	opts := *o
	opts.onPlacementsRemovedFn = value
	return &opts
}

func (o *activeStagedPlacementOptions) OnPlacementsRemovedFn() services.OnPlacementsRemovedFn {
	return o.onPlacementsRemovedFn
}
