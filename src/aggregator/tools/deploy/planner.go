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
	"fmt"
	"sort"
	"sync"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
)

var (
	emptyPlan deploymentPlan
	emptyStep deploymentStep
)

// planner generates deployment plans for given instances under constraints.
type planner interface {
	// GeneratePlan generates a deployment plan for given target instances.
	GeneratePlan(toDeploy, all instanceMetadatas) (deploymentPlan, error)

	// GenerateOneStep generates one deployment step for given target instances.
	GenerateOneStep(toDeploy, all instanceMetadatas) (deploymentStep, error)
}

type deploymentPlanner struct {
	leaderService    services.LeaderService
	workers          xsync.WorkerPool
	electionKeyFmt   string
	maxStepSize      int
	validatorFactory validatorFactory
}

// newPlanner creates a new deployment planner.
func newPlanner(client AggregatorClient, opts PlannerOptions) planner {
	workers := opts.WorkerPool()
	validatorFactory := newValidatorFactory(client, workers)
	return deploymentPlanner{
		leaderService:    opts.LeaderService(),
		workers:          opts.WorkerPool(),
		electionKeyFmt:   opts.ElectionKeyFmt(),
		maxStepSize:      opts.MaxStepSize(),
		validatorFactory: validatorFactory,
	}
}

func (p deploymentPlanner) GeneratePlan(
	toDeploy, all instanceMetadatas,
) (deploymentPlan, error) {
	grouped, err := p.groupInstancesByShardSetID(toDeploy, all)
	if err != nil {
		return emptyPlan, fmt.Errorf("unable to group instances by shard set id: %v", err)
	}
	return p.generatePlan(grouped, len(toDeploy), p.maxStepSize), nil
}

func (p deploymentPlanner) GenerateOneStep(
	toDeploy, all instanceMetadatas,
) (deploymentStep, error) {
	grouped, err := p.groupInstancesByShardSetID(toDeploy, all)
	if err != nil {
		return emptyStep, fmt.Errorf("unable to group instances by shard set id: %v", err)
	}
	return p.generateStep(grouped, p.maxStepSize), nil
}

func (p deploymentPlanner) generatePlan(
	instances map[uint32]*instanceGroup,
	numInstances int,
	maxStepSize int,
) deploymentPlan {
	var (
		step  deploymentStep
		plan  deploymentPlan
		total = numInstances
	)
	for total > 0 {
		step = p.generateStep(instances, maxStepSize)
		plan.Steps = append(plan.Steps, step)
		total -= len(step.Targets)
	}
	return plan
}

func (p deploymentPlanner) generateStep(
	instances map[uint32]*instanceGroup,
	maxStepSize int,
) deploymentStep {
	// NB(xichen): we always choose instances that are currently in the follower state first,
	// unless there are no more follower instances, in which case we'll deploy the leader instances.
	// This is to reduce the overall deployment time due to reduced number of leader promotions and
	// as such we are less likely to need to wait for the follower instances to be ready to take over
	// the leader role.
	step := p.generateStepFromTargetType(instances, maxStepSize, followerTarget)

	// If we have found some follower instances to deploy, we don't attempt to deploy leader
	// instances in the same step even if we have not reached the max step size to avoid delaying
	// deploying to the followers due to deploying leader instances.
	if len(step.Targets) > 0 {
		return step
	}

	// If we have not found any followers, we proceed to deploy leader instances.
	return p.generateStepFromTargetType(instances, maxStepSize, leaderTarget)
}

func (p deploymentPlanner) generateStepFromTargetType(
	instances map[uint32]*instanceGroup,
	maxStepSize int,
	targetType targetType,
) deploymentStep {
	step := deploymentStep{Targets: make([]deploymentTarget, 0, maxStepSize)}
	for shardSetID, group := range instances {
		if len(group.ToDeploy) == 0 {
			delete(instances, shardSetID)
			continue
		}

		done := false
		for i, instance := range group.ToDeploy {
			if !matchTargetType(instance.PlacementInstanceID, group.LeaderID, targetType) {
				continue
			}
			target := deploymentTarget{
				Instance:  instance,
				Validator: p.validatorFactory.ValidatorFor(instance, group, targetType),
			}
			step.Targets = append(step.Targets, target)
			group.removeInstanceToDeploy(i)
			if maxStepSize != 0 && len(step.Targets) >= maxStepSize {
				done = true
			}
			break
		}
		if done {
			break
		}
	}

	// Sort targets by instance id for deterministic ordering.
	sort.Sort(targetsByInstanceIDAsc(step.Targets))
	return step
}

func (p deploymentPlanner) groupInstancesByShardSetID(
	toDeploy, all instanceMetadatas,
) (map[uint32]*instanceGroup, error) {
	grouped := make(map[uint32]*instanceGroup, len(toDeploy))

	// Group the instances to be deployed by shard set id.
	for _, instance := range toDeploy {
		shardSetID := instance.ShardSetID
		group, exists := grouped[shardSetID]
		if !exists {
			group = &instanceGroup{
				ToDeploy: make(instanceMetadatas, 0, 2),
				All:      make(instanceMetadatas, 0, 2),
			}
		}
		group.ToDeploy = append(group.ToDeploy, instance)
		grouped[shardSetID] = group
	}

	// Determine the full set of instances in each group.
	for _, instance := range all {
		shardSetID := instance.ShardSetID
		group, exists := grouped[shardSetID]
		if !exists {
			continue
		}
		group.All = append(group.All, instance)
	}

	// Determine the leader of each group.
	var (
		wg    sync.WaitGroup
		errCh = make(chan error, len(grouped))
	)
	for shardSetID, group := range grouped {
		shardSetID, group := shardSetID, group
		wg.Add(1)
		p.workers.Go(func() {
			defer wg.Done()

			electionKey := fmt.Sprintf(p.electionKeyFmt, shardSetID)
			leader, err := p.leaderService.Leader(electionKey)
			if err != nil {
				err = fmt.Errorf("unable to determine leader for shard set id %d: %v", shardSetID, err)
				errCh <- err
				return
			}
			for _, instance := range group.All {
				if instance.PlacementInstanceID == leader {
					group.LeaderID = instance.PlacementInstanceID
					return
				}
			}
			err = fmt.Errorf("unknown leader %s for shard set id %d", leader, shardSetID)
			errCh <- err
		})
	}

	wg.Wait()
	close(errCh)
	multiErr := errors.NewMultiError()
	for err := range errCh {
		multiErr = multiErr.Add(err)
	}
	if err := multiErr.FinalError(); err != nil {
		return nil, err
	}
	return grouped, nil
}

// deploymentTarget is a deployment target.
type deploymentTarget struct {
	Instance  instanceMetadata
	Validator validator
}

func (t deploymentTarget) String() string { return t.Instance.PlacementInstanceID }

// deploymentTargets is a list of deployment targets.
type deploymentTargets []deploymentTarget

func (targets deploymentTargets) DeploymentInstanceIDs() []string {
	deploymentInstanceIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		deploymentInstanceIDs = append(deploymentInstanceIDs, target.Instance.DeploymentInstanceID)
	}
	return deploymentInstanceIDs
}

type targetsByInstanceIDAsc []deploymentTarget

func (a targetsByInstanceIDAsc) Len() int      { return len(a) }
func (a targetsByInstanceIDAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a targetsByInstanceIDAsc) Less(i, j int) bool {
	return a[i].Instance.PlacementInstanceID < a[j].Instance.PlacementInstanceID
}

// deploymentStep is a deployment step.
type deploymentStep struct {
	Targets deploymentTargets
}

// deploymentPlan is a deployment plan.
type deploymentPlan struct {
	Steps []deploymentStep
}

type targetType int

const (
	followerTarget targetType = iota
	leaderTarget
)

func matchTargetType(
	instanceID string,
	leaderID string,
	targetType targetType,
) bool {
	if targetType == leaderTarget {
		return instanceID == leaderID
	}
	return instanceID != leaderID
}

type instanceGroup struct {
	// LeaderID is the instance id of the leader in the group.
	LeaderID string

	// ToDeploy are the instances to be deployed in the group.
	ToDeploy instanceMetadatas

	// All include all the instances in the group regardless of whether they need to be deployed.
	All instanceMetadatas
}

func (group *instanceGroup) removeInstanceToDeploy(i int) {
	lastIdx := len(group.ToDeploy) - 1
	group.ToDeploy[i], group.ToDeploy[lastIdx] = group.ToDeploy[lastIdx], group.ToDeploy[i]
	group.ToDeploy = group.ToDeploy[:lastIdx]
}
