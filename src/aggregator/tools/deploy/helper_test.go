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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/x/retry"

	"github.com/golang/mock/gomock"
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	instances := testMockInstances(ctrl)
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()

	errGeneratePlan := errors.New("error generating plan")
	planner := NewMockplanner(ctrl)
	planner.EXPECT().GeneratePlan(gomock.Any(), gomock.Any()).Return(emptyPlan, errGeneratePlan).AnyTimes()

	helper := testHelper(t)
	helper.mgr = mgr
	helper.planner = planner
	require.Error(t, helper.Deploy("revision4", testPlacement, DryRunMode))
}

func TestHelperGeneratePlanDryRunMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		filteredRes instanceMetadatas
		allRes      instanceMetadatas
	)

	instances := testMockInstances(ctrl)
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()

	planner := NewMockplanner(ctrl)
	planner.EXPECT().
		GeneratePlan(gomock.Any(), gomock.Any()).
		DoAndReturn(func(toDeploy, all instanceMetadatas) (deploymentPlan, error) {
			filteredRes = toDeploy
			allRes = all
			return emptyPlan, nil
		}).
		AnyTimes()

	helper := testHelper(t)
	helper.mgr = mgr
	helper.planner = planner
	require.NoError(t, helper.Deploy("revision4", testPlacement, DryRunMode))
	require.Equal(t, testInstanceMetadatas[:3], filteredRes)
	require.Equal(t, testInstanceMetadatas, allRes)
}

func TestHelperWaitUntilSafeQueryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errQuery := errors.New("error querying instances")
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(nil, errQuery).AnyTimes()

	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = mgr
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas))
}

func TestHelperWaitUntilSafeInstanceUnhealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsHealthy().Return(false).AnyTimes()
	instance1.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsHealthy().Return(false).AnyTimes()
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()

	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = mgr
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeInstanceIsDeploying(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance1.EXPECT().IsDeploying().Return(true).AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()

	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = mgr

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().IsHealthy(gomock.Any()).Return(nil).AnyTimes()
	helper.client = client
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeInstanceUnhealthyFromAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errInstanceUnhealthy := errors.New("instance is not healthy")
	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance1.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()

	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)
	helper.mgr = mgr

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().IsHealthy(gomock.Any()).Return(errInstanceUnhealthy).AnyTimes()
	helper.client = client
	require.Error(t, helper.waitUntilSafe(testInstanceMetadatas[:2]))
}

func TestHelperWaitUntilSafeSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance1.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsHealthy().Return(true).AnyTimes()
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()

	helper := testHelper(t)
	helper.mgr = mgr

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().IsHealthy(gomock.Any()).Return(nil).AnyTimes()
	helper.client = client
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().Resign(gomock.Any()).Return(errResign).AnyTimes()
	helper.client = client
	require.Error(t, helper.resign(targets))
}

func TestHelperResignSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targets := deploymentTargets{
		{Instance: testInstanceMetadatas[0]},
		{Instance: testInstanceMetadatas[1]},
	}
	helper := testHelper(t)
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().Resign(gomock.Any()).Return(nil).AnyTimes()
	helper.client = client
	require.NoError(t, helper.resign(targets))
}

func TestHelperWaitUntilProgressingQueryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetIDs := []string{"instance1", "instance2"}
	revision := "revision1"
	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)

	errQuery := errors.New("error querying instances")
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(nil, errQuery).AnyTimes()
	helper.mgr = mgr
	require.Error(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceNotProgressing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsDeploying().Return(false).AnyTimes()
	instance1.EXPECT().Revision().Return("revision1").AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instance2.EXPECT().Revision().Return("revision1").AnyTimes()
	instances = append(instances, instance2)

	helper := testHelper(t)
	retryOpts := retry.NewOptions().
		SetMaxRetries(3).
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(1)
	helper.foreverRetrier = retry.NewRetrier(retryOpts)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()
	helper.mgr = mgr
	require.Error(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceIsDeploying(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsDeploying().Return(true).AnyTimes()
	instance1.EXPECT().Revision().Return("revision1").AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instance2.EXPECT().Revision().Return("revision1").AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr
	require.NoError(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperWaitUntilProgressingInstanceIsDeployed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetIDs := []string{"instance1", "instance2"}
	revision := "revision2"

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().IsDeploying().Return(false).AnyTimes()
	instance1.EXPECT().Revision().Return("revision2").AnyTimes()
	instances = append(instances, instance1)

	instance2 := NewMockInstance(ctrl)
	instance2.EXPECT().IsDeploying().Return(false).AnyTimes()
	instance2.EXPECT().Revision().Return("revision1").AnyTimes()
	instances = append(instances, instance2)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().Query(gomock.Any()).Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr
	require.NoError(t, helper.waitUntilProgressing(targetIDs, revision))
}

func TestHelperAllInstanceMetadatasManagerQueryAllError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errQueryAll := errors.New("query all error")
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(nil, errQueryAll).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasNumInstancesMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instance1 := NewMockInstance(ctrl)
	instance1.EXPECT().ID().Return("instance1").AnyTimes()
	instances = append(instances, instance1)

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()

	helper := testHelper(t)
	helper.mgr = mgr
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasToAPIEndpointFnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errToAPIEndpoint := errors.New("error converting to api endpoint")
	instances := testMockInstances(ctrl)
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr
	helper.toAPIEndpointFn = func(string) (string, error) { return "", errToAPIEndpoint }
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasToPlacementInstanceIDFnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errToPlacementInstanceID := errors.New("error converting to placement instance id")
	instances := testMockInstances(ctrl)
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr
	helper.toPlacementInstanceIDFn = func(string) (string, error) { return "", errToPlacementInstanceID }
	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasDuplicateDeploymentInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instances = append(instances, testMockInstances(ctrl)...)
	instances[0] = instances[1]

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr

	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasDeploymentInstanceNotExist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instances []Instance
	instances = append(instances, testMockInstances(ctrl)...)
	instance := NewMockInstance(ctrl)
	instance.EXPECT().ID().Return("deployment_instance5").AnyTimes()
	instance.EXPECT().Revision().Return("revision5").AnyTimes()
	instances[3] = instance

	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr

	_, err := helper.allInstanceMetadatas(testPlacement)
	require.Error(t, err)
}

func TestHelperAllInstanceMetadatasSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	instances := testMockInstances(ctrl)
	mgr := NewMockManager(ctrl)
	mgr.EXPECT().QueryAll().Return(instances, nil).AnyTimes()
	helper := testHelper(t)
	helper.mgr = mgr

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

func testMockInstances(ctrl *gomock.Controller) []Instance {
	instances := make([]Instance, 4)
	for i := 1; i <= 4; i++ {
		instance := NewMockInstance(ctrl)
		instance.EXPECT().ID().Return(fmt.Sprintf("deployment_instance%d", i)).AnyTimes()
		instance.EXPECT().Revision().Return(fmt.Sprintf("revision%d", i)).AnyTimes()
		instances[i-1] = instance
	}
	return instances
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
