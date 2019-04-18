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

package deploy

import (
	"errors"
	"net/http"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/sync"
)

const (
	defaultSettleDurationBetweenSteps = time.Minute
	defaultHelperWorkerPoolSize       = 16
)

var (
	errNoPlannerOptions          = errors.New("no planner options")
	errNoManager                 = errors.New("no manager")
	errNoHTTPClient              = errors.New("no http client")
	errNoToPlacementInstanceIDFn = errors.New("no to placement instance id function")
	errNoToAPIEndpointFn         = errors.New("no to api endpoint function")
)

// ToPlacementInstanceIDFn converts a deployment instance id to the corresponding
// placement instance id.
type ToPlacementInstanceIDFn func(deploymentInstanceID string) (string, error)

// ToAPIEndpointFn converts a placement instance endpoint to the corresponding
// aggregator instance api endpoint.
type ToAPIEndpointFn func(placementEndpoint string) (string, error)

// HelperOptions provide a set of options for the deployment helper.
type HelperOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) HelperOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetPlannerOptions sets the deployment planner options.
	SetPlannerOptions(value PlannerOptions) HelperOptions

	// PlannerOptions returns the deployment planner options.
	PlannerOptions() PlannerOptions

	// SetManager sets the deployment manager.
	SetManager(value Manager) HelperOptions

	// Manager returns the deployment manager.
	Manager() Manager

	// SetHTTPClient sets the http client.
	SetHTTPClient(value *http.Client) HelperOptions

	// HTTPClient returns the http client.
	HTTPClient() *http.Client

	// SetRetryOptions sets the retry options.
	SetRetryOptions(value retry.Options) HelperOptions

	// RetryOptions returns the retry options.
	RetryOptions() retry.Options

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value sync.WorkerPool) HelperOptions

	// WorkerPool returns the worker pool.
	WorkerPool() sync.WorkerPool

	// SetToPlacementInstanceIDFn sets the function that converts a deployment
	// instance id to the corresponding placement instance id.
	SetToPlacementInstanceIDFn(value ToPlacementInstanceIDFn) HelperOptions

	// ToPlacementInstanceIDFn returns the function that converts a deployment
	// instance id to the corresponding placement instance id.
	ToPlacementInstanceIDFn() ToPlacementInstanceIDFn

	// SetToAPIEndpointFn sets the function that converts a placement
	// instance endpoint to the corresponding aggregator instance api endpoint.
	SetToAPIEndpointFn(value ToAPIEndpointFn) HelperOptions

	// ToAPIEndpointFn returns the function that converts a placement
	// instance endpoint to the corresponding aggregator instance api endpoint.
	ToAPIEndpointFn() ToAPIEndpointFn

	// SetSettleDurationBetweenSteps sets the settlement duration between consecutive steps.
	SetSettleDurationBetweenSteps(value time.Duration) HelperOptions

	// SettleDurationBetweenSteps returns the settlement duration between consecutive steps.
	SettleDurationBetweenSteps() time.Duration

	// Validate validates the options.
	Validate() error
}

type helperOptions struct {
	instrumentOpts          instrument.Options
	plannerOpts             PlannerOptions
	manager                 Manager
	httpClient              *http.Client
	retryOpts               retry.Options
	workerPool              sync.WorkerPool
	toPlacementInstanceIDFn ToPlacementInstanceIDFn
	toAPIEndpointFn         ToAPIEndpointFn
	settleDuration          time.Duration
}

// NewHelperOptions create a set of deployment helper options.
func NewHelperOptions() HelperOptions {
	workers := sync.NewWorkerPool(defaultHelperWorkerPoolSize)
	workers.Init()
	return &helperOptions{
		instrumentOpts: instrument.NewOptions(),
		retryOpts:      retry.NewOptions(),
		workerPool:     workers,
		settleDuration: defaultSettleDurationBetweenSteps,
	}
}

func (o *helperOptions) SetInstrumentOptions(value instrument.Options) HelperOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *helperOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *helperOptions) SetPlannerOptions(value PlannerOptions) HelperOptions {
	opts := *o
	opts.plannerOpts = value
	return &opts
}

func (o *helperOptions) PlannerOptions() PlannerOptions {
	return o.plannerOpts
}

func (o *helperOptions) SetManager(value Manager) HelperOptions {
	opts := *o
	opts.manager = value
	return &opts
}

func (o *helperOptions) Manager() Manager {
	return o.manager
}

func (o *helperOptions) SetHTTPClient(value *http.Client) HelperOptions {
	opts := *o
	opts.httpClient = value
	return &opts
}

func (o *helperOptions) HTTPClient() *http.Client {
	return o.httpClient
}

func (o *helperOptions) SetRetryOptions(value retry.Options) HelperOptions {
	opts := *o
	opts.retryOpts = value
	return &opts
}

func (o *helperOptions) RetryOptions() retry.Options {
	return o.retryOpts
}

func (o *helperOptions) SetWorkerPool(value sync.WorkerPool) HelperOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *helperOptions) WorkerPool() sync.WorkerPool {
	return o.workerPool
}

func (o *helperOptions) SetToPlacementInstanceIDFn(value ToPlacementInstanceIDFn) HelperOptions {
	opts := *o
	opts.toPlacementInstanceIDFn = value
	return &opts
}

func (o *helperOptions) ToPlacementInstanceIDFn() ToPlacementInstanceIDFn {
	return o.toPlacementInstanceIDFn
}

func (o *helperOptions) SetToAPIEndpointFn(value ToAPIEndpointFn) HelperOptions {
	opts := *o
	opts.toAPIEndpointFn = value
	return &opts
}

func (o *helperOptions) ToAPIEndpointFn() ToAPIEndpointFn {
	return o.toAPIEndpointFn
}

func (o *helperOptions) SetSettleDurationBetweenSteps(value time.Duration) HelperOptions {
	opts := *o
	opts.settleDuration = value
	return &opts
}

func (o *helperOptions) SettleDurationBetweenSteps() time.Duration {
	return o.settleDuration
}

func (o *helperOptions) Validate() error {
	if o.plannerOpts == nil {
		return errNoPlannerOptions
	}
	if o.manager == nil {
		return errNoManager
	}
	if o.httpClient == nil {
		return errNoHTTPClient
	}
	if o.toPlacementInstanceIDFn == nil {
		return errNoToPlacementInstanceIDFn
	}
	if o.toAPIEndpointFn == nil {
		return errNoToAPIEndpointFn
	}
	return nil
}
