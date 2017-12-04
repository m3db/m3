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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
)

var (
	testPlacement = placement.NewPlacement().SetInstances(
		[]placement.Instance{
			placement.NewInstance().
				SetID("placement_instance1").
				SetEndpoint("placement_instance1_endpoint").
				SetShardSetID(0),
			placement.NewInstance().
				SetID("placement_instance2").
				SetEndpoint("placement_instance2_endpoint").
				SetShardSetID(0),
			placement.NewInstance().
				SetID("placement_instance3").
				SetEndpoint("placement_instance3_endpoint").
				SetShardSetID(1),
			placement.NewInstance().
				SetID("placement_instance4").
				SetEndpoint("placement_instance4_endpoint").
				SetShardSetID(1),
		},
	)
	testMockInstances = []Instance{
		&mockInstance{id: "deployment_instance1", revision: "revision1"},
		&mockInstance{id: "deployment_instance2", revision: "revision2"},
		&mockInstance{id: "deployment_instance3", revision: "revision3"},
		&mockInstance{id: "deployment_instance4", revision: "revision4"},
	}
	testInstanceMetadatas = instanceMetadatas{
		{
			PlacementInstanceID:  "placement_instance1",
			DeploymentInstanceID: "deployment_instance1",
			ShardSetID:           0,
			APIEndpoint:          "placement_instance1_endpoint",
			Revision:             "revision1",
		},
		{
			PlacementInstanceID:  "placement_instance2",
			DeploymentInstanceID: "deployment_instance2",
			ShardSetID:           0,
			APIEndpoint:          "placement_instance2_endpoint",
			Revision:             "revision2",
		},
		{
			PlacementInstanceID:  "placement_instance3",
			DeploymentInstanceID: "deployment_instance3",
			ShardSetID:           1,
			APIEndpoint:          "placement_instance3_endpoint",
			Revision:             "revision3",
		},
		{
			PlacementInstanceID:  "placement_instance4",
			DeploymentInstanceID: "deployment_instance4",
			ShardSetID:           1,
			APIEndpoint:          "placement_instance4_endpoint",
			Revision:             "revision4",
		},
	}
)

func TestHelperDeployEmptyRevision(t *testing.T) {
	helper := testHelper(t)
	require.Equal(t, errInvalidRevision, helper.Deploy("", nil, DryRunMode))
}

func TestHelperGeneratePlanError(t *testing.T) {
	errGeneratePlan := errors.New("error generating plan")
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return testMockInstances, nil },
	}
	helper.planner = &mockPlanner{
		generatePlanFn: func(toDeploy, all instanceMetadatas) (deploymentPlan, error) {
			return emptyPlan, errGeneratePlan
		},
	}
	require.Error(t, helper.Deploy("revision4", testPlacement, DryRunMode))
}

func TestHelperGeneratePlanDryRunMode(t *testing.T) {
	var (
		filteredRes instanceMetadatas
		allRes      instanceMetadatas
	)
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return testMockInstances, nil },
	}
	helper.planner = &mockPlanner{
		generatePlanFn: func(toDeploy, all instanceMetadatas) (deploymentPlan, error) {
			filteredRes = toDeploy
			allRes = all
			return emptyPlan, nil
		},
	}
	require.NoError(t, helper.Deploy("revision4", testPlacement, DryRunMode))
	require.Equal(t, testInstanceMetadatas[:3], filteredRes)
	require.Equal(t, testInstanceMetadatas, allRes)
}

func TestHelperWaitUntilSafeQueryError(t *testing.T) {
	errQuery := errors.New("error querying instances")
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) {
			return nil, errQuery
		},
	}
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas))
}

func TestHelperWaitUntilSafeInstanceUnhealthy(t *testing.T) {
	instances := []Instance{
		&mockInstance{isHealthy: false, isDeploying: false},
		&mockInstance{isHealthy: false, isDeploying: false},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) { return instances, nil },
	}
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeInstanceIsDeploying(t *testing.T) {
	instances := []Instance{
		&mockInstance{isHealthy: true, isDeploying: true},
		&mockInstance{isHealthy: true, isDeploying: false},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) { return instances, nil },
	}
	helper.client = &mockAggregatorClient{
		isHealthyFn: func(string) error { return nil },
	}
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeInstanceUnhealthyFromAPI(t *testing.T) {
	errInstanceUnhealthy := errors.New("instance is not healthy")
	instances := []Instance{
		&mockInstance{isHealthy: true, isDeploying: false},
		&mockInstance{isHealthy: true, isDeploying: false},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) { return instances, nil },
	}
	helper.client = &mockAggregatorClient{
		isHealthyFn: func(string) error { return errInstanceUnhealthy },
	}
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeSuccess(t *testing.T) {
	instances := []Instance{
		&mockInstance{isHealthy: true, isDeploying: false},
		&mockInstance{isHealthy: true, isDeploying: false},
	}
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) { return instances, nil },
	}
	helper.client = &mockAggregatorClient{
		isHealthyFn: func(string) error { return nil },
	}
	require.NoError(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperValidateError(t *testing.T) {
	errValidate := errors.New("error validating")
	targets := deploymentTargets{
		{Validator: func() error { return errValidate }},
		{Validator: func() error { return errValidate }},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	require.Error(t, helper.validate(targets))
}

func TestHelperValidateSuccess(t *testing.T) {
	targets := deploymentTargets{
		{Validator: func() error { return nil }},
		{Validator: func() error { return nil }},
	}
	helper := testHelper(t)
	require.NoError(t, helper.validate(targets))
}

func TestHelperResignError(t *testing.T) {
	errResign := errors.New("error resigning")
	targets := deploymentTargets{
		{Instance: testInstanceMetadatas[0]},
		{Instance: testInstanceMetadatas[1]},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.retrier = retry.NewRetrier(retryOpts)
	helper.client = &mockAggregatorClient{
		resignFn: func(string) error { return errResign },
	}
	require.Error(t, helper.resign(targets))
}

func TestHelperResignSuccess(t *testing.T) {
	targets := deploymentTargets{
		{Instance: testInstanceMetadatas[0]},
		{Instance: testInstanceMetadatas[1]},
	}
	helper := testHelper(t)
	helper.client = &mockAggregatorClient{
		resignFn: func(string) error { return nil },
	}
	require.NoError(t, helper.resign(targets))
}

func TestHelperWaitUntilProgressingQueryError(t *testing.T) {
	errQuery := errors.New("error querying instances")
	targetIDs := []string{"instance1", "instance2"}
	revision := "revision1"
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) {
			return nil, errQuery
		},
	}
	require.Error(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceNotProgressing(t *testing.T) {
	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"
	targetInstances := []Instance{
		&mockInstance{isDeploying: false, revision: "revision1"},
		&mockInstance{isDeploying: false, revision: "revision1"},
	}
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) {
			return targetInstances, nil
		},
	}
	require.Error(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceIsDeploying(t *testing.T) {
	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"
	targetInstances := []Instance{
		&mockInstance{isDeploying: true, revision: "revision1"},
		&mockInstance{isDeploying: false, revision: "revision1"},
	}
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) {
			return targetInstances, nil
		},
	}
	require.NoError(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceIsDeployed(t *testing.T) {
	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"
	targetInstances := []Instance{
		&mockInstance{isDeploying: false, revision: "revision2"},
		&mockInstance{isDeploying: false, revision: "revision1"},
	}
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryFn: func([]string) ([]Instance, error) {
			return targetInstances, nil
		},
	}
	require.NoError(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperAllInstanceMetadatasManagerQueryAllError(t *testing.T) {
	errQueryAll := errors.New("query all error")
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return nil, errQueryAll },
	}
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasNumInstancesMismatch(t *testing.T) {
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) {
			return []Instance{&mockInstance{id: "instance1"}}, nil
		},
	}
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasToAPIEndpointFnError(t *testing.T) {
	errToAPIEndpoint := errors.New("error converting to api endpoint")
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return testMockInstances, nil },
	}
	helper.toAPIEndpointFn = func(string) (string, error) { return "", errToAPIEndpoint }
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasToPlacementInstanceIDFnError(t *testing.T) {
	errToPlacementInstanceID := errors.New("error converting to placement instance id")
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return testMockInstances, nil },
	}
	helper.toPlacementInstanceIDFn = func(string) (string, error) { return "", errToPlacementInstanceID }
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasDuplicateDeploymentInstance(t *testing.T) {
	var mockInstances []Instance
	mockInstances = append(mockInstances, testMockInstances...)
	mockInstances[0] = mockInstances[1]
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return mockInstances, nil },
	}
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasDeploymentInstanceNotExist(t *testing.T) {
	var mockInstances []Instance
	mockInstances = append(mockInstances, testMockInstances...)
	mockInstances[3] = &mockInstance{id: "deployment_instance5", revision: "revision5"}
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return mockInstances, nil },
	}
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasSuccess(t *testing.T) {
	helper := testHelper(t)
	helper.mgr = &mockManager{
		queryAllFn: func() ([]Instance, error) { return testMockInstances, nil },
	}
	res, err := helper.allInstanceMetadatas(testPlacement)
	require.NoError(t, err)
	require.Equal(t, testInstanceMetadatas, res)
}

func TestInstanceMetadatasDeploymentInstanceIDs(t *testing.T) {
	expectedIDs := []string{
		"deployment_instance1",
		"deployment_instance2",
		"deployment_instance3",
		"deployment_instance4",
	}
	require.Equal(t, expectedIDs, testInstanceMetadatas.DeploymentInstanceIDs())
}

func TestInstanceMetadatasFilter(t *testing.T) {
	require.Equal(t, instanceMetadatas(testInstanceMetadatas[1:]), testInstanceMetadatas.WithoutRevision("revision1"))
}

func testHelper(t *testing.T) helper {
	toAPIEndpointFn := func(endpoint string) (string, error) { return endpoint, nil }
	toPlacementInstanceIDFn := func(id string) (string, error) {
		converted := strings.Replace(id, "deployment", "placement", -1)
		return converted, nil
	}
	opts := NewHelperOptions().
		SetPlannerOptions(NewPlannerOptions()).
		SetToAPIEndpointFn(toAPIEndpointFn).
		SetToPlacementInstanceIDFn(toPlacementInstanceIDFn)
	res, err := NewHelper(opts)
	require.NoError(t, err)
	return res.(helper)
}
