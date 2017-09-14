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
	"fmt"
	"time"

	"github.com/m3db/m3em/build"
	"github.com/m3db/m3em/node"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/instrument"
	xretry "github.com/m3db/m3x/retry"
)

const (
	defaultSessionOverride      = false
	defaultReplication          = 3
	defaultConcurrency          = 10
	defaultNumShards            = 1024
	defaultNodeOperationTimeout = 2 * time.Minute
)

type clusterOpts struct {
	iopts            instrument.Options
	sessionOverride  bool
	token            string
	svcBuild         build.ServiceBuild
	svcConf          build.ServiceConfiguration
	placementSvc     placement.Service
	placementRetrier xretry.Retrier
	replication      int
	numShards        int
	concurrency      int
	nodeOpTimeout    time.Duration
	listener         node.Listener
}

// NewOptions returns a new Options object
func NewOptions(
	placementSvc placement.Service,
	iopts instrument.Options,
) Options {
	if iopts == nil {
		iopts = instrument.NewOptions()
	}
	return clusterOpts{
		iopts:            iopts,
		sessionOverride:  defaultSessionOverride,
		replication:      defaultReplication,
		numShards:        defaultNumShards,
		concurrency:      defaultConcurrency,
		nodeOpTimeout:    defaultNodeOperationTimeout,
		placementSvc:     placementSvc,
		placementRetrier: defaultRetrier(),
	}
}

func defaultRetrier() xretry.Retrier {
	opts := xretry.NewOptions().
		SetForever(true).
		SetMaxBackoff(time.Second).
		SetMaxRetries(10)
	return xretry.NewRetrier(opts)
}

func (o clusterOpts) Validate() error {
	if o.token == "" {
		return fmt.Errorf("no session token set")
	}

	if o.svcBuild == nil {
		return fmt.Errorf("ServiceBuild is not set")
	}

	if o.svcConf == nil {
		return fmt.Errorf("ServiceConf is not set")
	}

	if o.placementSvc == nil {
		return fmt.Errorf("PlacementService is not set")
	}

	return nil
}

func (o clusterOpts) SetInstrumentOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}

func (o clusterOpts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o clusterOpts) SetServiceBuild(b build.ServiceBuild) Options {
	o.svcBuild = b
	return o
}

func (o clusterOpts) ServiceBuild() build.ServiceBuild {
	return o.svcBuild
}

func (o clusterOpts) SetServiceConfig(c build.ServiceConfiguration) Options {
	o.svcConf = c
	return o
}

func (o clusterOpts) ServiceConfig() build.ServiceConfiguration {
	return o.svcConf
}

func (o clusterOpts) SetSessionToken(t string) Options {
	o.token = t
	return o
}

func (o clusterOpts) SessionToken() string {
	return o.token
}

func (o clusterOpts) SetSessionOverride(override bool) Options {
	o.sessionOverride = override
	return o
}

func (o clusterOpts) SessionOverride() bool {
	return o.sessionOverride
}

func (o clusterOpts) SetReplication(r int) Options {
	o.replication = r
	return o
}

func (o clusterOpts) Replication() int {
	return o.replication
}

func (o clusterOpts) SetNumShards(ns int) Options {
	o.numShards = ns
	return o
}

func (o clusterOpts) NumShards() int {
	return o.numShards
}

func (o clusterOpts) SetPlacementService(psvc placement.Service) Options {
	o.placementSvc = psvc
	return o
}

func (o clusterOpts) PlacementService() placement.Service {
	return o.placementSvc
}

func (o clusterOpts) SetPlacementServiceRetrier(r xretry.Retrier) Options {
	o.placementRetrier = r
	return o
}

func (o clusterOpts) PlacementServiceRetrier() xretry.Retrier {
	return o.placementRetrier
}

func (o clusterOpts) SetNodeConcurrency(c int) Options {
	o.concurrency = c
	return o
}

func (o clusterOpts) NodeConcurrency() int {
	return o.concurrency
}

func (o clusterOpts) SetNodeOperationTimeout(t time.Duration) Options {
	o.nodeOpTimeout = t
	return o
}

func (o clusterOpts) NodeOperationTimeout() time.Duration {
	return o.nodeOpTimeout
}

func (o clusterOpts) SetNodeListener(l node.Listener) Options {
	o.listener = l
	return o
}

func (o clusterOpts) NodeListener() node.Listener {
	return o.listener
}
