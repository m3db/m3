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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/retry"
	xsync "github.com/m3db/m3/src/x/sync"

	"go.uber.org/zap"
)

var (
	errNoDeploymentProgress = errors.New("no deployment progress")
	errInvalidRevision      = errors.New("invalid revision")
)

// Mode is the deployment mode.
type Mode int

// A list of supported deployment modes.
const (
	DryRunMode Mode = iota
	ForceMode
)

// Helper is a helper class handling deployments.
type Helper interface {
	// Deploy deploys a target revision to the instances in the placement.
	Deploy(revision string, placement placement.Placement, mode Mode) error
}

// TODO(xichen): disable deployment while another is ongoing.
type helper struct {
	logger                  *zap.Logger
	planner                 planner
	client                  AggregatorClient
	mgr                     Manager
	retrier                 retry.Retrier
	foreverRetrier          retry.Retrier
	workers                 xsync.WorkerPool
	toPlacementInstanceIDFn ToPlacementInstanceIDFn
	toAPIEndpointFn         ToAPIEndpointFn
	settleBetweenSteps      time.Duration
}

// NewHelper creates a new deployment helper.
func NewHelper(opts HelperOptions) (Helper, error) {
	client := NewAggregatorClient(opts.HTTPClient())
	planner := newPlanner(client, opts.PlannerOptions())
	retryOpts := opts.RetryOptions()
	retrier := retry.NewRetrier(retryOpts)
	foreverRetrier := retry.NewRetrier(retryOpts.SetForever(true))
	return helper{
		logger:                  opts.InstrumentOptions().Logger(),
		planner:                 planner,
		client:                  client,
		mgr:                     opts.Manager(),
		retrier:                 retrier,
		foreverRetrier:          foreverRetrier,
		workers:                 opts.WorkerPool(),
		toPlacementInstanceIDFn: opts.ToPlacementInstanceIDFn(),
		toAPIEndpointFn:         opts.ToAPIEndpointFn(),
		settleBetweenSteps:      opts.SettleDurationBetweenSteps(),
	}, nil
}

func (h helper) Deploy(revision string, placement placement.Placement, mode Mode) error {
	if revision == "" {
		return errInvalidRevision
	}
	all, err := h.allInstanceMetadatas(placement)
	if err != nil {
		return fmt.Errorf("unable to get all instance metadatas: %v", err)
	}
	filtered := all.WithoutRevision(revision)

	plan, err := h.planner.GeneratePlan(filtered, all)
	if err != nil {
		return fmt.Errorf("unable to generate deployment plan: %v", err)
	}

	h.logger.Sugar().Info("generated deployment plan: %+v", plan)

	// If in dry run mode, log the generated deployment plan and return.
	if mode == DryRunMode {
		return nil
	}

	if err = h.execute(plan, revision, all); err != nil {
		return fmt.Errorf("unable to execute deployment plan: %v", err)
	}

	return nil
}

func (h helper) execute(
	plan deploymentPlan,
	revision string,
	all instanceMetadatas,
) error {
	numSteps := len(plan.Steps)
	for i, step := range plan.Steps {
		h.logger.Sugar().Infof("deploying step %d of %d", i+1, numSteps)
		if err := h.executeStep(step, revision, all); err != nil {
			return err
		}
		h.logger.Sugar().Infof("deploying step %d succeeded", i+1)
		if h.settleBetweenSteps > 0 {
			h.logger.Sugar().Infof("waiting settle duration after step: %s", h.settleBetweenSteps.String())
			time.Sleep(h.settleBetweenSteps)
		}
	}
	return nil
}

func (h helper) executeStep(
	step deploymentStep,
	revision string,
	all instanceMetadatas,
) error {
	h.logger.Sugar().Infof("waiting until safe to deploy for step %v", step)
	if err := h.waitUntilSafe(all); err != nil {
		return err
	}

	h.logger.Sugar().Infof("waiting until all targets are validated for step %v", step)
	if err := h.validate(step.Targets); err != nil {
		return err
	}

	h.logger.Sugar().Infof("waiting until all targets have resigned for step %v", step)
	if err := h.resign(step.Targets); err != nil {
		return err
	}

	h.logger.Sugar().Infof("beginning to deploy instances for step %v", step)
	targetIDs := step.Targets.DeploymentInstanceIDs()
	if err := h.deploy(targetIDs, revision); err != nil {
		return err
	}

	h.logger.Sugar().Infof("deployment started, waiting for progress: %v", step)
	if err := h.waitUntilProgressing(targetIDs, revision); err != nil {
		return err
	}

	h.logger.Sugar().Infof("deployment progressed, waiting for completion: %v", step)
	return h.waitUntilSafe(all)
}

func (h helper) waitUntilSafe(instances instanceMetadatas) error {
	deploymentInstanceIDs := instances.DeploymentInstanceIDs()
	return h.foreverRetrier.Attempt(func() error {
		deploymentInstances, err := h.mgr.Query(deploymentInstanceIDs)
		if err != nil {
			return fmt.Errorf("error querying instances: %v", err)
		}

		var (
			wg   sync.WaitGroup
			safe int64
		)
		for i := range deploymentInstances {
			i := i
			wg.Add(1)
			h.workers.Go(func() {
				defer wg.Done()

				if !deploymentInstances[i].IsHealthy() || deploymentInstances[i].IsDeploying() {
					return
				}
				if err := h.client.IsHealthy(instances[i].APIEndpoint); err != nil {
					return
				}
				atomic.AddInt64(&safe, 1)
			})
		}
		wg.Wait()

		if safe != int64(len(instances)) {
			return fmt.Errorf("only %d out of %d instances are safe to deploy", safe, len(instances))
		}
		return nil
	})
}

func (h helper) validate(targets deploymentTargets) error {
	return h.forEachTarget(targets, func(target deploymentTarget) error {
		return h.foreverRetrier.Attempt(func() error {
			validator := target.Validator
			if validator == nil {
				return nil
			}
			if err := validator(); err != nil {
				err = fmt.Errorf("validation error for instance %s: %v", target.Instance.PlacementInstanceID, err)
				return err
			}
			return nil
		})
	})
}

func (h helper) resign(targets deploymentTargets) error {
	return h.forEachTarget(targets, func(target deploymentTarget) error {
		return h.retrier.Attempt(func() error {
			instance := target.Instance
			if err := h.client.Resign(instance.APIEndpoint); err != nil {
				err = fmt.Errorf("resign error for instance %s: %v", instance.PlacementInstanceID, err)
				return err
			}
			return nil
		})
	})
}

func (h helper) deploy(targetIDs []string, revision string) error {
	return h.retrier.Attempt(func() error {
		return h.mgr.Deploy(targetIDs, revision)
	})
}

func (h helper) waitUntilProgressing(targetIDs []string, revision string) error {
	return h.foreverRetrier.Attempt(func() error {
		targetInstances, err := h.mgr.Query(targetIDs)
		if err != nil {
			return fmt.Errorf("error querying instances: %v", err)
		}

		for _, di := range targetInstances {
			if di.IsDeploying() || di.Revision() == revision {
				return nil
			}
		}

		return errNoDeploymentProgress
	})
}

func (h helper) forEachTarget(targets deploymentTargets, workFn targetWorkFn) error {
	var (
		wg    sync.WaitGroup
		errCh = make(chan error, len(targets))
	)
	for i := range targets {
		i := i
		wg.Add(1)
		h.workers.Go(func() {
			defer wg.Done()

			if err := workFn(targets[i]); err != nil {
				errCh <- err
			}
		})
	}
	wg.Wait()
	close(errCh)

	multiErr := xerrors.NewMultiError()
	for err := range errCh {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}

func (h helper) allInstanceMetadatas(placement placement.Placement) (instanceMetadatas, error) {
	placementInstances := placement.Instances()
	deploymentInstances, err := h.mgr.QueryAll()
	if err != nil {
		return nil, fmt.Errorf("unable to query all instances from deployment: %v", err)
	}
	metadatas, err := h.computeInstanceMetadatas(placementInstances, deploymentInstances)
	if err != nil {
		return nil, fmt.Errorf("unable to compute instance metadatas: %v", err)
	}
	return metadatas, nil
}

// validateInstances validates instances derived from placement against
// instances derived from deployment, ensuring there are no duplicate instances
// and the instances derived from two sources match against each other.
func (h helper) computeInstanceMetadatas(
	placementInstances []placement.Instance,
	deploymentInstances []Instance,
) (instanceMetadatas, error) {
	if len(placementInstances) != len(deploymentInstances) {
		errMsg := "number of instances is %d in the placement and %d in the deployment"
		return nil, fmt.Errorf(errMsg, len(placementInstances), len(deploymentInstances))
	}

	// Populate instance metadata from placement information.
	metadatas := make(instanceMetadatas, len(placementInstances))
	unique := make(map[string]int)
	for i, pi := range placementInstances {
		id := pi.ID()
		_, exists := unique[id]
		if exists {
			return nil, fmt.Errorf("instance %s not unique in the placement", id)
		}
		endpoint := pi.Endpoint()
		apiEndpoint, err := h.toAPIEndpointFn(endpoint)
		if err != nil {
			return nil, fmt.Errorf("unable to convert placement endpoint %s to api endpoint: %v", endpoint, err)
		}
		unique[id] = i
		metadatas[i].PlacementInstanceID = id
		metadatas[i].ShardSetID = pi.ShardSetID()
		metadatas[i].APIEndpoint = apiEndpoint
	}

	// Populate instance metadata from deployment information.
	for _, di := range deploymentInstances {
		id := di.ID()
		placementInstanceID, err := h.toPlacementInstanceIDFn(id)
		if err != nil {
			return nil, fmt.Errorf("unable to convert deployment instance id %s to placement instance id", id)
		}
		idx, exists := unique[placementInstanceID]
		if !exists {
			return nil, fmt.Errorf("instance %s is in deployment but not in placement", id)
		}
		if metadatas[idx].DeploymentInstanceID != "" {
			return nil, fmt.Errorf("instance %s not unique in the deployment", id)
		}
		metadatas[idx].DeploymentInstanceID = id
		metadatas[idx].Revision = di.Revision()
	}

	return metadatas, nil
}

type targetWorkFn func(target deploymentTarget) error

// instanceMetadata contains instance metadata.
type instanceMetadata struct {
	// PlacementInstanceID is the instance id in the placement.
	PlacementInstanceID string

	// DeploymentInstanceID is the instance id in the deployment system.
	DeploymentInstanceID string

	// ShardSetID is the shard set id associated with the instance.
	ShardSetID uint32

	// APIEndpoint is the api endpoint for the instance.
	APIEndpoint string

	// Revision is the revision deployed to the instance.
	Revision string
}

type instanceMetadatas []instanceMetadata

func (m instanceMetadatas) DeploymentInstanceIDs() []string {
	res := make([]string, 0, len(m))
	for _, metadata := range m {
		res = append(res, metadata.DeploymentInstanceID)
	}
	return res
}

func (m instanceMetadatas) WithoutRevision(revision string) instanceMetadatas {
	filtered := make(instanceMetadatas, 0, len(m))
	for _, metadata := range m {
		if metadata.Revision == revision {
			continue
		}
		filtered = append(filtered, metadata)
	}
	return filtered
}
