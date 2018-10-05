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

package cluster

import (
	"time"

	"github.com/m3db/m3x/instrument"
	xretry "github.com/m3db/m3x/retry"
)

// Configuration is a YAML wrapper around Options
type Configuration struct {
	SessionToken            *string               `yaml:"sessionToken"`
	SessionOverride         *bool                 `yaml:"sessionOverride"`
	Relication              *int                  `yaml:"replication"`
	NumShards               *int                  `yaml:"numShards"`
	PlacementServiceRetrier *xretry.Configuration `yaml:"placementServiceRetrier"`
	NodeConcurrency         *int                  `yaml:"nodeConcurrency"`
	NodeOperationTimeout    *time.Duration        `yaml:"nodeOperationTimeout"`
}

// Options returns `Options` corresponding to the provided struct values
func (c *Configuration) Options(iopts instrument.Options) Options {
	opts := NewOptions(nil, iopts)
	if t := c.SessionToken; t != nil && *t != "" {
		opts = opts.SetSessionToken(*t)
	}
	if t := c.SessionOverride; t != nil {
		opts = opts.SetSessionOverride(*t)
	}
	if t := c.Relication; t != nil && *t > 0 {
		opts = opts.SetReplication(*t)
	}
	if t := c.NumShards; t != nil && *t > 0 {
		opts = opts.SetNumShards(*t)
	}
	if t := c.PlacementServiceRetrier; t != nil {
		opts = opts.SetPlacementServiceRetrier(
			c.PlacementServiceRetrier.NewRetrier(iopts.MetricsScope()))
	}
	if t := c.NodeConcurrency; t != nil && *t > 0 {
		opts = opts.SetNodeConcurrency(*t)
	}
	if t := c.NodeOperationTimeout; t != nil && *t > 0 {
		opts = opts.SetNodeOperationTimeout(*t)
	}
	return opts
}
