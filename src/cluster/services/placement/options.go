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

package placement

import (
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultMaxStepSize = 3
	defaultIsSharded   = true
	// by default partial replace should be allowed for better distribution
	defaultAllowPartialReplace = true
)

type deploymentOptions struct {
	maxStepSize int
}

// NewDeploymentOptions returns a default DeploymentOptions
func NewDeploymentOptions() DeploymentOptions {
	return deploymentOptions{maxStepSize: defaultMaxStepSize}
}

func (o deploymentOptions) MaxStepSize() int {
	return o.maxStepSize
}

func (o deploymentOptions) SetMaxStepSize(stepSize int) DeploymentOptions {
	o.maxStepSize = stepSize
	return o
}

// NewOptions returns a default PlacementOptions
func NewOptions() services.PlacementOptions {
	return options{
		allowPartialReplace: defaultAllowPartialReplace,
		sharded:             defaultIsSharded,
		iopts:               instrument.NewOptions(),
	}
}

type options struct {
	looseRackCheck      bool
	allowPartialReplace bool
	sharded             bool
	dryrun              bool
	iopts               instrument.Options
}

func (o options) LooseRackCheck() bool {
	return o.looseRackCheck
}

func (o options) SetLooseRackCheck(looseRackCheck bool) services.PlacementOptions {
	o.looseRackCheck = looseRackCheck
	return o
}

func (o options) AllowPartialReplace() bool {
	return o.allowPartialReplace
}

func (o options) SetAllowPartialReplace(allowPartialReplace bool) services.PlacementOptions {
	o.allowPartialReplace = allowPartialReplace
	return o
}

func (o options) IsSharded() bool {
	return o.sharded
}

func (o options) SetIsSharded(sharded bool) services.PlacementOptions {
	o.sharded = sharded
	return o
}

func (o options) Dryrun() bool {
	return o.dryrun
}

func (o options) SetDryrun(d bool) services.PlacementOptions {
	o.dryrun = d
	return o
}

func (o options) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentOptions(iopts instrument.Options) services.PlacementOptions {
	o.iopts = iopts
	return o
}
