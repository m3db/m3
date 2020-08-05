// Copyright (c) 2019 Uber Technologies, Inc.
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

package handleroptions

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/headers"
)

const (
	// M3DBServiceName is the service name for M3DB.
	M3DBServiceName = "m3db"
	// M3AggregatorServiceName is the service name for M3Aggregator.
	M3AggregatorServiceName = "m3aggregator"
	// M3CoordinatorServiceName is the service name for M3Coordinator.
	M3CoordinatorServiceName = "m3coordinator"

	defaultM3AggMaxAggregationWindowSize = time.Minute
	// defaultM3AggWarmupDuration configures the buffer to account for the delay
	// of propagating aggregator placement to clients, usually needed when there is
	// a large amount of clients sending traffic to m3aggregator.
	defaultM3AggWarmupDuration = 0
)

var (
	errServiceNameIsRequired        = errors.New("service name is required")
	errServiceEnvironmentIsRequired = errors.New("service environment is required")
	errServiceZoneIsRequired        = errors.New("service zone is required")
	errM3AggServiceOptionsRequired  = errors.New("m3agg service options are required")
)

// ServiceOptions are the options for Service.
type ServiceOptions struct {
	ServiceName        string
	ServiceEnvironment string
	ServiceZone        string

	M3Agg *M3AggServiceOptions

	DryRun bool
	Force  bool
}

// M3AggServiceOptions contains the service options that are
// specific to the M3Agg service.
type M3AggServiceOptions struct {
	MaxAggregationWindowSize time.Duration
	WarmupDuration           time.Duration
}

// ServiceOptionsDefault is a default to apply to service options.
type ServiceOptionsDefault func(o ServiceOptions) ServiceOptions

// WithDefaultServiceEnvironment returns the default service environment.
func WithDefaultServiceEnvironment(env string) ServiceOptionsDefault {
	return func(o ServiceOptions) ServiceOptions {
		o.ServiceEnvironment = env
		return o
	}
}

// WithDefaultServiceZone returns the default service zone.
func WithDefaultServiceZone(zone string) ServiceOptionsDefault {
	return func(o ServiceOptions) ServiceOptions {
		o.ServiceZone = zone
		return o
	}
}

// ServiceNameAndDefaults is the params used when identifying a service
// and it's service option defaults.
type ServiceNameAndDefaults struct {
	ServiceName string
	Defaults    []ServiceOptionsDefault
}

// NewServiceOptions returns a ServiceOptions based on the provided
// values.
func NewServiceOptions(
	service ServiceNameAndDefaults,
	header http.Header,
	m3AggOpts *M3AggServiceOptions,
) ServiceOptions {
	opts := ServiceOptions{
		ServiceName:        service.ServiceName,
		ServiceEnvironment: headers.DefaultServiceEnvironment,
		ServiceZone:        headers.DefaultServiceZone,

		DryRun: false,
		Force:  false,

		M3Agg: &M3AggServiceOptions{
			MaxAggregationWindowSize: defaultM3AggMaxAggregationWindowSize,
			WarmupDuration:           defaultM3AggWarmupDuration,
		},
	}
	for _, applyDefault := range service.Defaults {
		opts = applyDefault(opts)
	}

	if v := strings.TrimSpace(header.Get(headers.HeaderClusterEnvironmentName)); v != "" {
		opts.ServiceEnvironment = v
	}
	if v := strings.TrimSpace(header.Get(headers.HeaderClusterZoneName)); v != "" {
		opts.ServiceZone = v
	}
	if v := strings.TrimSpace(header.Get(headers.HeaderDryRun)); v == "true" {
		opts.DryRun = true
	}
	if v := strings.TrimSpace(header.Get(headers.HeaderForce)); v == "true" {
		opts.Force = true
	}

	if m3AggOpts != nil {
		if m3AggOpts.MaxAggregationWindowSize > 0 {
			opts.M3Agg.MaxAggregationWindowSize = m3AggOpts.MaxAggregationWindowSize
		}

		if m3AggOpts.WarmupDuration > 0 {
			opts.M3Agg.WarmupDuration = m3AggOpts.WarmupDuration
		}
	}

	return opts
}

// Validate ensures the service options are valid.
func (opts *ServiceOptions) Validate() error {
	if opts.ServiceName == "" {
		return errServiceNameIsRequired
	}
	if opts.ServiceEnvironment == "" {
		return errServiceEnvironmentIsRequired
	}
	if opts.ServiceZone == "" {
		return errServiceZoneIsRequired
	}
	if opts.ServiceName == M3AggregatorServiceName && opts.M3Agg == nil {
		return errM3AggServiceOptionsRequired
	}
	return nil
}

// ServiceID constructs a cluster services ID from the options.
func (opts *ServiceOptions) ServiceID() services.ServiceID {
	return services.NewServiceID().
		SetName(opts.ServiceName).
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)
}

// KVOverrideOptions constructs KV overrides from the current service options.
func (opts *ServiceOptions) KVOverrideOptions() kv.OverrideOptions {
	return kv.NewOverrideOptions().
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)
}
