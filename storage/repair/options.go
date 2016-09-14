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

	"github.com/m3db/m3db/client"
)

const (
	defaultRepairInterval      = 2 * time.Hour
	defaultRepairTimeOffset    = time.Hour
	defaultRepairCheckInterval = time.Minute
)

var (
	errNoAdminClient              = errors.New("no admin client in repair options")
	errInvalidRepairInterval      = errors.New("invalid repair interval in repair options")
	errInvalidRepairTimeOffset    = errors.New("invalid repair time offset in repair options")
	errRepairTimeOffsetTooBig     = errors.New("repair time offset too big in repair options")
	errInvalidRepairCheckInterval = errors.New("invalid repair check interval in repair options")
	errRepairCheckIntervalTooBig  = errors.New("repair check interval too big in repair options")
)

type options struct {
	adminClient         client.AdminClient
	repairInterval      time.Duration
	repairTimeOffset    time.Duration
	repairCheckInterval time.Duration
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		repairInterval:      defaultRepairInterval,
		repairTimeOffset:    defaultRepairTimeOffset,
		repairCheckInterval: defaultRepairCheckInterval,
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

func (o *options) SetRepairInterval(value time.Duration) Options {
	opts := *o
	opts.repairInterval = value
	return &opts
}

func (o *options) RepairInterval() time.Duration {
	return o.repairInterval
}

func (o *options) SetRepairTimeOffset(value time.Duration) Options {
	opts := *o
	opts.repairTimeOffset = value
	return &opts
}

func (o *options) RepairTimeOffset() time.Duration {
	return o.repairTimeOffset
}

func (o *options) SetRepairCheckInterval(value time.Duration) Options {
	opts := *o
	opts.repairCheckInterval = value
	return &opts
}

func (o *options) RepairCheckInterval() time.Duration {
	return o.repairCheckInterval
}

func (o *options) Validate() error {
	if o.adminClient == nil {
		return errNoAdminClient
	}
	if o.repairInterval < 0 {
		return errInvalidRepairInterval
	}
	if o.repairTimeOffset < 0 {
		return errInvalidRepairTimeOffset
	}
	if o.repairTimeOffset > o.repairInterval {
		return errRepairTimeOffsetTooBig
	}
	if o.repairCheckInterval < 0 {
		return errInvalidRepairCheckInterval
	}
	if o.repairCheckInterval > o.repairInterval {
		return errRepairCheckIntervalTooBig
	}
	return nil
}
