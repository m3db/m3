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
	"sort"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"

	"github.com/stretchr/testify/require"
)

var (
	testElectionKeyFmt    = "/shardset/%d"
	testInstancesToDeploy = instanceMetadatas{
		instanceMetadata{
			PlacementInstanceID: "instance1",
			ShardSetID:          0,
		},
		instanceMetadata{
			PlacementInstanceID: "instance2",
			ShardSetID:          0,
		},
		instanceMetadata{
			PlacementInstanceID: "instance3",
			ShardSetID:          1,
		},
	}
	testAllInstances = instanceMetadatas{
		instanceMetadata{
			PlacementInstanceID: "instance1",
			ShardSetID:          0,
		},
		instanceMetadata{
			PlacementInstanceID: "instance2",
			ShardSetID:          0,
		},
		instanceMetadata{
			PlacementInstanceID: "instance3",
			ShardSetID:          1,
		},
		instanceMetadata{
			PlacementInstanceID: "instance4",
			ShardSetID:          1,
		},
	}
)

func TestGeneratePlan(t *testing.T) {
	var validators []capturingValidator
	electionIDForShardset0 := fmt.Sprintf(testElectionKeyFmt, 0)
	electionIDForShardset1 := fmt.Sprintf(testElectionKeyFmt, 1)
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			if electionID == electionIDForShardset0 {
				return "instance1", nil
			}
			if electionID == electionIDForShardset1 {
				return "instance3", nil
			}
			return "", errors.New("unrecognized election id")
		},
	}
	opts := NewPlannerOptions().
		SetLeaderService(leaderService).
		SetElectionKeyFmt(testElectionKeyFmt)
	planner := newPlanner(nil, opts).(deploymentPlanner)
	planner.validatorFactory = &mockValidatorFactory{
		validatorForFn: func(
			instance instanceMetadata,
			group *instanceGroup,
			targetType targetType,
		) validator {
			validators = append(validators, capturingValidator{
				instance:   instance,
				group:      group,
				targetType: targetType,
			})
			return nil
		},
	}
	plan, err := planner.GeneratePlan(testInstancesToDeploy, testAllInstances)
	require.NoError(t, err)

	i := 0
	for _, step := range plan.Steps {
		for _, target := range step.Targets {
			require.Equal(t, validators[i].instance, target.Instance)
			i++
		}
	}

	expected := deploymentPlan{
		Steps: []deploymentStep{
			{
				Targets: []deploymentTarget{
					{
						Instance: instanceMetadata{
							PlacementInstanceID: "instance2",
							ShardSetID:          0,
						},
					},
				},
			},
			{
				Targets: []deploymentTarget{
					{
						Instance: instanceMetadata{
							PlacementInstanceID: "instance1",
							ShardSetID:          0,
						},
					},
					{
						Instance: instanceMetadata{
							PlacementInstanceID: "instance3",
							ShardSetID:          1,
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, plan)
}

func TestGeneratePlanWithStepSizeLimit(t *testing.T) {
	var validators []capturingValidator
	electionIDForShardset0 := fmt.Sprintf(testElectionKeyFmt, 0)
	electionIDForShardset1 := fmt.Sprintf(testElectionKeyFmt, 1)
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			if electionID == electionIDForShardset0 {
				return "instance1", nil
			}
			if electionID == electionIDForShardset1 {
				return "instance3", nil
			}
			return "", errors.New("unrecognized election id")
		},
	}
	opts := NewPlannerOptions().
		SetLeaderService(leaderService).
		SetElectionKeyFmt(testElectionKeyFmt).
		SetMaxStepSize(1)
	planner := newPlanner(nil, opts).(deploymentPlanner)
	planner.validatorFactory = &mockValidatorFactory{
		validatorForFn: func(
			instance instanceMetadata,
			group *instanceGroup,
			targetType targetType,
		) validator {
			validators = append(validators, capturingValidator{
				instance:   instance,
				group:      group,
				targetType: targetType,
			})
			return nil
		},
	}
	plan, err := planner.GeneratePlan(testInstancesToDeploy, testAllInstances)
	require.NoError(t, err)

	i := 0
	for _, step := range plan.Steps {
		for _, target := range step.Targets {
			require.Equal(t, validators[i].instance, target.Instance)
			i++
		}
	}

	step1 := deploymentStep{
		Targets: []deploymentTarget{
			{
				Instance: instanceMetadata{
					PlacementInstanceID: "instance2",
					ShardSetID:          0,
				},
			},
		},
	}
	step2 := deploymentStep{
		Targets: []deploymentTarget{
			{
				Instance: instanceMetadata{
					PlacementInstanceID: "instance1",
					ShardSetID:          0,
				},
			},
		},
	}
	step3 := deploymentStep{
		Targets: []deploymentTarget{
			{
				Instance: instanceMetadata{
					PlacementInstanceID: "instance3",
					ShardSetID:          1,
				},
			},
		},
	}
	require.Equal(t, 3, len(plan.Steps))
	var expected deploymentPlan
	if plan.Steps[1].Targets[0].Instance.PlacementInstanceID == "instance1" {
		expected = deploymentPlan{Steps: []deploymentStep{step1, step2, step3}}
	} else {
		expected = deploymentPlan{Steps: []deploymentStep{step1, step3, step2}}
	}
	require.Equal(t, expected, plan)
}

func TestGroupInstancesByShardSetID(t *testing.T) {
	electionIDForShardset0 := fmt.Sprintf(testElectionKeyFmt, 0)
	electionIDForShardset1 := fmt.Sprintf(testElectionKeyFmt, 1)
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			if electionID == electionIDForShardset0 {
				return "instance1", nil
			}
			if electionID == electionIDForShardset1 {
				return "instance3", nil
			}
			return "", errors.New("unrecognized election id")
		},
	}
	opts := NewPlannerOptions().
		SetLeaderService(leaderService).
		SetElectionKeyFmt(testElectionKeyFmt)
	planner := newPlanner(nil, opts).(deploymentPlanner)
	group, err := planner.groupInstancesByShardSetID(testInstancesToDeploy, testAllInstances)
	require.NoError(t, err)

	expectedGroup := map[uint32]*instanceGroup{
		0: &instanceGroup{
			LeaderID: "instance1",
			ToDeploy: instanceMetadatas{
				instanceMetadata{
					PlacementInstanceID: "instance1",
					ShardSetID:          0,
				},
				instanceMetadata{
					PlacementInstanceID: "instance2",
					ShardSetID:          0,
				},
			},
			All: instanceMetadatas{
				instanceMetadata{
					PlacementInstanceID: "instance1",
					ShardSetID:          0,
				},
				instanceMetadata{
					PlacementInstanceID: "instance2",
					ShardSetID:          0,
				},
			},
		},
		1: &instanceGroup{
			LeaderID: "instance3",
			ToDeploy: instanceMetadatas{
				instanceMetadata{
					PlacementInstanceID: "instance3",
					ShardSetID:          1,
				},
			},
			All: instanceMetadatas{
				instanceMetadata{
					PlacementInstanceID: "instance3",
					ShardSetID:          1,
				},
				instanceMetadata{
					PlacementInstanceID: "instance4",
					ShardSetID:          1,
				},
			},
		},
	}
	require.Equal(t, expectedGroup, group)
}

func TestGroupInstancesByShardSetIDLeaderError(t *testing.T) {
	errLeader := errors.New("leader error")
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "", errLeader
		},
	}
	opts := NewPlannerOptions().
		SetLeaderService(leaderService).
		SetElectionKeyFmt(testElectionKeyFmt)
	planner := newPlanner(nil, opts).(deploymentPlanner)
	_, err := planner.groupInstancesByShardSetID(testInstancesToDeploy, testAllInstances)
	require.Error(t, err)
}

func TestGroupInstancesByShardSetIDUnknownLeader(t *testing.T) {
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "nonexistent", nil
		},
	}
	opts := NewPlannerOptions().
		SetLeaderService(leaderService).
		SetElectionKeyFmt(testElectionKeyFmt)
	planner := newPlanner(nil, opts).(deploymentPlanner)
	_, err := planner.groupInstancesByShardSetID(testInstancesToDeploy, testAllInstances)
	require.Error(t, err)
}

func TestRemoveInstanceToDeploy(t *testing.T) {
	metadatas := instanceMetadatas{
		instanceMetadata{PlacementInstanceID: "instance1"},
		instanceMetadata{PlacementInstanceID: "instance2"},
		instanceMetadata{PlacementInstanceID: "instance3"},
	}
	group := &instanceGroup{
		ToDeploy: metadatas,
	}
	group.removeInstanceToDeploy(1)
	expected := instanceMetadatas{
		instanceMetadata{PlacementInstanceID: "instance1"},
		instanceMetadata{PlacementInstanceID: "instance3"},
	}
	require.Equal(t, group.ToDeploy, expected)
}

func TestTargetsByInstanceIDAsc(t *testing.T) {
	targets := []deploymentTarget{
		{
			Instance: instanceMetadata{
				PlacementInstanceID: "instance3",
			},
		},
		{
			Instance: instanceMetadata{
				PlacementInstanceID: "instance1",
			},
		},
		{
			Instance: instanceMetadata{
				PlacementInstanceID: "instance2",
			},
		},
		{
			Instance: instanceMetadata{
				PlacementInstanceID: "instance4",
			},
		},
	}

	expected := []deploymentTarget{targets[1], targets[2], targets[0], targets[3]}
	sort.Sort(targetsByInstanceIDAsc(targets))
	require.Equal(t, expected, targets)
}

type leaderCampaignFn func(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error)

type leaderResignFn func(electionID string) error
type leaderFn func(electionID string) (string, error)

type mockLeaderService struct {
	campaignFn leaderCampaignFn
	resignFn   leaderResignFn
	leaderFn   leaderFn
}

func (s *mockLeaderService) Campaign(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error) {
	return s.campaignFn(electionID, opts)
}

func (s *mockLeaderService) Resign(electionID string) error {
	return s.resignFn(electionID)
}

func (s *mockLeaderService) Leader(electionID string) (string, error) {
	return s.leaderFn(electionID)
}

func (s *mockLeaderService) Close() error { return nil }

type generatePlanFn func(toDeploy, all instanceMetadatas) (deploymentPlan, error)
type generateOneStepFn func(toDeploy, all instanceMetadatas) (deploymentStep, error)

type mockPlanner struct {
	generatePlanFn    generatePlanFn
	generateOneStepFn generateOneStepFn
}

func (m *mockPlanner) GeneratePlan(toDeploy, all instanceMetadatas) (deploymentPlan, error) {
	return m.generatePlanFn(toDeploy, all)
}

func (m *mockPlanner) GenerateOneStep(toDeploy, all instanceMetadatas) (deploymentStep, error) {
	return m.generateOneStepFn(toDeploy, all)
}
