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
	"time"

	"gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/instrument"
)

// Configuration is configuration for placement options.
type Configuration struct {
	AllowPartialReplace *bool           `yaml:"allowPartialReplace"`
	AllowAllZones       *bool           `yaml:"allowAllZones"`
	AddAllCandidates    *bool           `yaml:"addAllCandidates"`
	IsSharded           *bool           `yaml:"isSharded"`
	ShardStateMode      *ShardStateMode `yaml:"shardStateMode"`
	IsMirrored          *bool           `yaml:"isMirrored"`
	SkipPortMirroring   *bool           `yaml:"skipPortMirroring"`
	IsStaged            *bool           `yaml:"isStaged"`
	ValidZone           *string         `yaml:"validZone"`
}

// NewOptions creates a placement options.
func (c *Configuration) NewOptions() Options {
	opts := NewOptions()
	if value := c.AllowPartialReplace; value != nil {
		opts = opts.SetAllowPartialReplace(*value)
	}
	if value := c.AllowAllZones; value != nil {
		opts = opts.SetAllowAllZones(*value)
	}
	if value := c.AddAllCandidates; value != nil {
		opts = opts.SetAddAllCandidates(*value)
	}
	if value := c.IsSharded; value != nil {
		opts = opts.SetIsSharded(*value)
	}
	if value := c.ShardStateMode; value != nil {
		opts = opts.SetShardStateMode(*value)
	}
	if value := c.IsMirrored; value != nil {
		opts = opts.SetIsMirrored(*value)
	}
	if value := c.SkipPortMirroring; value != nil {
		opts = opts.SetSkipPortMirroring(*value)
	}
	if value := c.IsStaged; value != nil {
		opts = opts.SetIsStaged(*value)
	}
	if value := c.ValidZone; value != nil {
		opts = opts.SetValidZone(*value)
	}
	return opts
}

// DeepCopy makes a deep copy of the configuration.
func (c Configuration) DeepCopy() (Configuration, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		return Configuration{}, err
	}
	var res Configuration
	if err := yaml.Unmarshal(b, &res); err != nil {
		return Configuration{}, err
	}
	return res, nil
}

// ApplyOverride applys the override values.
func (c Configuration) ApplyOverride(opts *placementpb.Options) Configuration {
	if opts == nil {
		return c
	}
	if opts.IsSharded != nil {
		isShardedValueCopy := opts.IsSharded.Value
		c.IsSharded = &isShardedValueCopy
	}
	if opts.SkipPortMirroring != nil {
		skipPortMirroringCopy := opts.SkipPortMirroring.Value
		c.SkipPortMirroring = &skipPortMirroringCopy
	}
	return c
}

// WatcherConfiguration contains placement watcher configuration.
type WatcherConfiguration struct {
	// Placement key.
	Key string `yaml:"key" validate:"nonzero"`

	// Initial watch timeout.
	InitWatchTimeout time.Duration `yaml:"initWatchTimeout"`
}

// NewOptions creates a placement watcher option.
func (c *WatcherConfiguration) NewOptions(
	store kv.Store,
	instrumentOpts instrument.Options,
) WatcherOptions {
	opts := NewWatcherOptions().
		SetInstrumentOptions(instrumentOpts).
		SetStagedPlacementKey(c.Key).
		SetStagedPlacementStore(store)
	if c.InitWatchTimeout != 0 {
		opts = opts.SetInitWatchTimeout(c.InitWatchTimeout)
	}
	return opts
}
