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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
)

const (
	defaultSettleDurationBetweenSteps = time.Minute
	defaultHelperWorkerPoolSize       = 16
)

var (
	errNoPlannerOptions                = errors.New("no planner options")
	errNoManager                       = errors.New("no manager")
	errNoHTTPClient                    = errors.New("no http client")
	errNoKVStore                       = errors.New("no kv store")
	errNoToPlacementInstanceIDFn       = errors.New("no to placement instance id function")
	errNoToAPIEndpointFn               = errors.New("no to api endpoint function")
	errNoStagedPlacementWatcherOptions = errors.New("no staged placement watcher options")
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

	// SetKVStore sets the kv store.
	SetKVStore(value kv.Store) HelperOptions

	// KVStore returns the kv store.
	KVStore() kv.Store

	// SetRetryOptions sets the retry options.
	SetRetryOptions(value xretry.Options) HelperOptions

	// RetryOptions returns the retry options.
	RetryOptions() xretry.Options

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value xsync.WorkerPool) HelperOptions

	// WorkerPool returns the worker pool.
	WorkerPool() xsync.WorkerPool

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

	// SetStagedPlacementWatcherOptions sets the staged placement watcher options.
	SetStagedPlacementWatcherOptions(value placement.StagedPlacementWatcherOptions) HelperOptions

	// StagedPlacementWatcherOptions returns the staged placement watcher options.
	StagedPlacementWatcherOptions() placement.StagedPlacementWatcherOptions

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
	store                   kv.Store
	retryOpts               xretry.Options
	workerPool              xsync.WorkerPool
	toPlacementInstanceIDFn ToPlacementInstanceIDFn
	toAPIEndpointFn         ToAPIEndpointFn
	watcherOpts             placement.StagedPlacementWatcherOptions
	settleDuration          time.Duration
}

// NewHelperOptions create a set of deployment helper options.
func NewHelperOptions() HelperOptions {
	workers := xsync.NewWorkerPool(defaultHelperWorkerPoolSize)
	workers.Init()
	return &helperOptions{
		instrumentOpts: instrument.NewOptions(),
		retryOpts:      xretry.NewOptions(),
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

func (o *helperOptions) SetKVStore(value kv.Store) HelperOptions {
	opts := *o
	opts.store = value
	return &opts
}

func (o *helperOptions) KVStore() kv.Store {
	return o.store
}

func (o *helperOptions) SetRetryOptions(value xretry.Options) HelperOptions {
	opts := *o
	opts.retryOpts = value
	return &opts
}

func (o *helperOptions) RetryOptions() xretry.Options {
	return o.retryOpts
}

func (o *helperOptions) SetWorkerPool(value xsync.WorkerPool) HelperOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *helperOptions) WorkerPool() xsync.WorkerPool {
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

func (o *helperOptions) SetStagedPlacementWatcherOptions(value placement.StagedPlacementWatcherOptions) HelperOptions {
	opts := *o
	opts.watcherOpts = value
	return &opts
}

func (o *helperOptions) StagedPlacementWatcherOptions() placement.StagedPlacementWatcherOptions {
	return o.watcherOpts
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
	if o.store == nil {
		return errNoKVStore
	}
	if o.toPlacementInstanceIDFn == nil {
		return errNoToPlacementInstanceIDFn
	}
	if o.toAPIEndpointFn == nil {
		return errNoToAPIEndpointFn
	}
	if o.watcherOpts == nil {
		return errNoStagedPlacementWatcherOptions
	}
	return nil
}
