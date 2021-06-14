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

package repair

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
)

const (
	// Allow repairs to progress when a single peer is down (I.E during single node failure
	// or deployments).
	defaultRepairConsistencyLevel           = topology.ReadConsistencyLevelUnstrictMajority
	defaultRepairCheckInterval              = time.Minute
	defaultRepairThrottle                   = 90 * time.Second
	defaultRepairShardConcurrency           = 1
	defaultDebugShadowComparisonsEnabled    = false
	defaultDebugShadowComparisonsPercentage = 1.0
)

var (
	errNoAdminClient                           = errors.New("no admin client in repair options")
	errInvalidRepairCheckInterval              = errors.New("invalid repair check interval in repair options")
	errInvalidRepairThrottle                   = errors.New("invalid repair throttle in repair options")
	errNoReplicaMetadataSlicePool              = errors.New("no replica metadata pool in repair options")
	errNoResultOptions                         = errors.New("no result options in repair options")
	errInvalidDebugShadowComparisonsPercentage = errors.New("debug shadow comparisons percentage must be between 0 and 1")
)

type options struct {
	repairType                       Type
	force                            bool
	adminClients                     []client.AdminClient
	repairConsistencyLevel           topology.ReadConsistencyLevel
	repairShardConcurrency           int
	repairCheckInterval              time.Duration
	repairThrottle                   time.Duration
	replicaMetadataSlicePool         ReplicaMetadataSlicePool
	resultOptions                    result.Options
	debugShadowComparisonsEnabled    bool
	debugShadowComparisonsPercentage float64
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		repairConsistencyLevel:           defaultRepairConsistencyLevel,
		repairShardConcurrency:           defaultRepairShardConcurrency,
		repairCheckInterval:              defaultRepairCheckInterval,
		repairThrottle:                   defaultRepairThrottle,
		replicaMetadataSlicePool:         NewReplicaMetadataSlicePool(nil, 0),
		resultOptions:                    result.NewOptions(),
		debugShadowComparisonsEnabled:    defaultDebugShadowComparisonsEnabled,
		debugShadowComparisonsPercentage: defaultDebugShadowComparisonsPercentage,
	}
}

func (o *options) SetType(value Type) Options {
	opts := *o
	opts.repairType = value
	return &opts
}

func (o *options) Type() Type {
	return o.repairType
}

func (o *options) SetForce(value bool) Options {
	opts := *o
	opts.force = value
	return &opts
}

func (o *options) Force() bool {
	return o.force
}

func (o *options) SetAdminClients(value []client.AdminClient) Options {
	opts := *o
	opts.adminClients = value
	return &opts
}

func (o *options) AdminClients() []client.AdminClient {
	return o.adminClients
}

func (o *options) SetRepairConsistencyLevel(value topology.ReadConsistencyLevel) Options {
	opts := *o
	opts.repairConsistencyLevel = value
	return &opts
}

func (o *options) RepairConsistencyLevel() topology.ReadConsistencyLevel {
	return o.repairConsistencyLevel
}

func (o *options) SetRepairShardConcurrency(value int) Options {
	opts := *o
	opts.repairShardConcurrency = value
	return &opts
}

func (o *options) RepairShardConcurrency() int {
	return o.repairShardConcurrency
}

func (o *options) SetRepairCheckInterval(value time.Duration) Options {
	opts := *o
	opts.repairCheckInterval = value
	return &opts
}

func (o *options) RepairCheckInterval() time.Duration {
	return o.repairCheckInterval
}

func (o *options) SetRepairThrottle(value time.Duration) Options {
	opts := *o
	opts.repairThrottle = value
	return &opts
}

func (o *options) RepairThrottle() time.Duration {
	return o.repairThrottle
}

func (o *options) SetReplicaMetadataSlicePool(value ReplicaMetadataSlicePool) Options {
	opts := *o
	opts.replicaMetadataSlicePool = value
	return &opts
}

func (o *options) ReplicaMetadataSlicePool() ReplicaMetadataSlicePool {
	return o.replicaMetadataSlicePool
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOptions = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOptions
}

func (o *options) SetDebugShadowComparisonsEnabled(value bool) Options {
	opts := *o
	opts.debugShadowComparisonsEnabled = value
	return &opts
}

func (o *options) DebugShadowComparisonsEnabled() bool {
	return o.debugShadowComparisonsEnabled
}

func (o *options) SetDebugShadowComparisonsPercentage(value float64) Options {
	opts := *o
	opts.debugShadowComparisonsPercentage = value
	return &opts
}

func (o *options) DebugShadowComparisonsPercentage() float64 {
	return o.debugShadowComparisonsPercentage
}

func (o *options) Validate() error {
	if len(o.adminClients) == 0 {
		return errNoAdminClient
	}

	var prevOrigin string
	for _, c := range o.adminClients {
		currOrigin := c.Options().(client.AdminOptions).Origin().ID()
		if prevOrigin == "" {
			prevOrigin = currOrigin
			continue
		}

		if currOrigin != prevOrigin {
			return fmt.Errorf(
				"all repair clients should have the same origin, prev: %s, curr: %s",
				prevOrigin, currOrigin)
		}
	}

	if o.repairCheckInterval < 0 {
		return errInvalidRepairCheckInterval
	}
	if o.repairThrottle < 0 {
		return errInvalidRepairThrottle
	}
	if o.replicaMetadataSlicePool == nil {
		return errNoReplicaMetadataSlicePool
	}
	if o.resultOptions == nil {
		return errNoResultOptions
	}
	if o.debugShadowComparisonsPercentage > 1.0 ||
		o.debugShadowComparisonsPercentage < 0 {
		return errInvalidDebugShadowComparisonsPercentage
	}
	return nil
}
