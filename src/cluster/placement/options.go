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
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultMaxStepSize = 3
	defaultIsSharded   = true
	// By default partial replace should be allowed for better distribution.
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

func defaultTimeNanosFn() int64 { return shard.UnInitializedValue }

type options struct {
	looseRackCheck      bool
	allowPartialReplace bool
	isSharded           bool
	isMirrored          bool
	isStaged            bool
	iopts               instrument.Options
	validZone           string
	dryrun              bool
	placementCutOverFn  TimeNanosFn
	shardCutOverFn      TimeNanosFn
	shardCutOffFn       TimeNanosFn
}

// NewOptions returns a default Options.
func NewOptions() Options {
	return options{
		allowPartialReplace: defaultAllowPartialReplace,
		isSharded:           defaultIsSharded,
		iopts:               instrument.NewOptions(),
		placementCutOverFn:  defaultTimeNanosFn,
		shardCutOverFn:      defaultTimeNanosFn,
		shardCutOffFn:       defaultTimeNanosFn,
	}
}

func (o options) LooseRackCheck() bool {
	return o.looseRackCheck
}

func (o options) SetLooseRackCheck(looseRackCheck bool) Options {
	o.looseRackCheck = looseRackCheck
	return o
}

func (o options) AllowPartialReplace() bool {
	return o.allowPartialReplace
}

func (o options) SetAllowPartialReplace(allowPartialReplace bool) Options {
	o.allowPartialReplace = allowPartialReplace
	return o
}

func (o options) IsSharded() bool {
	return o.isSharded
}

func (o options) SetIsSharded(sharded bool) Options {
	o.isSharded = sharded
	return o
}

func (o options) IsMirrored() bool {
	return o.isMirrored
}

func (o options) SetIsMirrored(v bool) Options {
	o.isMirrored = v
	return o
}

func (o options) IsStaged() bool {
	return o.isStaged
}

func (o options) SetIsStaged(v bool) Options {
	o.isStaged = v
	return o
}

func (o options) Dryrun() bool {
	return o.dryrun
}

func (o options) SetDryrun(d bool) Options {
	o.dryrun = d
	return o
}

func (o options) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}

func (o options) ValidZone() string {
	return o.validZone
}

func (o options) SetValidZone(z string) Options {
	o.validZone = z
	return o
}

func (o options) PlacementCutoverNanosFn() TimeNanosFn {
	return o.placementCutOverFn
}

func (o options) SetPlacementCutoverNanosFn(fn TimeNanosFn) Options {
	o.placementCutOverFn = fn
	return o
}

func (o options) ShardCutoverNanosFn() TimeNanosFn {
	return o.shardCutOverFn
}

func (o options) SetShardCutoverNanosFn(fn TimeNanosFn) Options {
	o.shardCutOverFn = fn
	return o
}

func (o options) ShardCutoffNanosFn() TimeNanosFn {
	return o.shardCutOffFn
}

func (o options) SetShardCutoffNanosFn(fn TimeNanosFn) Options {
	o.shardCutOffFn = fn
	return o
}
