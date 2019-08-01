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
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/topology"
)

const (
	defaultRepairConsistencyLevel           = topology.ReadConsistencyLevelMajority
	defaultRepairInterval                   = 2 * time.Hour
	defaultRepairCheckInterval              = time.Minute
	defaultRepairThrottle                   = 90 * time.Second
	defaultRepairShardConcurrency           = 1
	defaultDebugShadowComparisonsEnabled    = false
	defaultDebugShadowComparisonsPercentage = 1.0
)

var (
	errNoAdminClient                           = errors.New("no admin client in repair options")
	errInvalidRepairInterval                   = errors.New("invalid repair interval in repair options")
	errInvalidRepairCheckInterval              = errors.New("invalid repair check interval in repair options")
	errRepairCheckIntervalTooBig               = errors.New("repair check interval too big in repair options")
	errInvalidRepairThrottle                   = errors.New("invalid repair throttle in repair options")
	errNoHostBlockMetadataSlicePool            = errors.New("no host block metadata pool in repair options")
	errInvalidDebugShadowComparisonsPercentage = errors.New("debug shadow comparisons percentage must be between 0 and 1")
)

type options struct {
	adminClient                      client.AdminClient
	repairConsistencyLevel           topology.ReadConsistencyLevel
	repairShardConcurrency           int
	repairInterval                   time.Duration
	repairCheckInterval              time.Duration
	repairThrottle                   time.Duration
	hostBlockMetadataSlicePool       HostBlockMetadataSlicePool
	debugShadowComparisonsEnabled    bool
	debugShadowComparisonsPercentage float64
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		repairConsistencyLevel:           defaultRepairConsistencyLevel,
		repairShardConcurrency:           defaultRepairShardConcurrency,
		repairInterval:                   defaultRepairInterval,
		repairCheckInterval:              defaultRepairCheckInterval,
		repairThrottle:                   defaultRepairThrottle,
		hostBlockMetadataSlicePool:       NewHostBlockMetadataSlicePool(nil, 0),
		debugShadowComparisonsEnabled:    defaultDebugShadowComparisonsEnabled,
		debugShadowComparisonsPercentage: defaultDebugShadowComparisonsPercentage,
	}
}

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.adminClient = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.adminClient
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

func (o *options) SetRepairInterval(value time.Duration) Options {
	opts := *o
	opts.repairInterval = value
	return &opts
}

func (o *options) RepairInterval() time.Duration {
	return o.repairInterval
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

func (o *options) SetHostBlockMetadataSlicePool(value HostBlockMetadataSlicePool) Options {
	opts := *o
	opts.hostBlockMetadataSlicePool = value
	return &opts
}

func (o *options) HostBlockMetadataSlicePool() HostBlockMetadataSlicePool {
	return o.hostBlockMetadataSlicePool
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
	if o.adminClient == nil {
		return errNoAdminClient
	}
	if o.repairInterval < 0 {
		return errInvalidRepairInterval
	}
	if o.repairCheckInterval < 0 {
		return errInvalidRepairCheckInterval
	}
	if o.repairCheckInterval > o.repairInterval {
		return errRepairCheckIntervalTooBig
	}
	if o.repairThrottle < 0 {
		return errInvalidRepairThrottle
	}
	if o.hostBlockMetadataSlicePool == nil {
		return errNoHostBlockMetadataSlicePool
	}
	if o.debugShadowComparisonsPercentage > 1.0 ||
		o.debugShadowComparisonsPercentage < 0 {
		return errInvalidDebugShadowComparisonsPercentage
	}
	return nil
}
