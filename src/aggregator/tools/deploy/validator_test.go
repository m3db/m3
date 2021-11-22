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
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestValidatorForFollowerStatusError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errStatus := errors.New("status error")
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().Status(gomock.Any()).Return(aggregator.RuntimeStatus{}, errStatus).AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instanceMetadata{}, nil, followerTarget)
	require.Equal(t, errStatus, validator())
}

func TestValidatorForFollowerNotFollowerState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{
			FlushStatus: aggregator.FlushStatus{
				ElectionState: aggregator.LeaderState,
			},
		}, nil).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instanceMetadata{}, nil, followerTarget)
	require.Error(t, validator())
}

func TestValidatorForFollowerSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{
			FlushStatus: aggregator.FlushStatus{
				ElectionState: aggregator.FollowerState,
			},
		}, nil).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instanceMetadata{}, nil, followerTarget)
	require.NoError(t, validator())
}

func TestValidatorForLeaderStatusError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errStatus := errors.New("status error")
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{}, errStatus).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instanceMetadata{}, nil, leaderTarget)
	require.Equal(t, errStatus, validator())
}

func TestValidatorForLeaderNotLeaderState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{
			FlushStatus: aggregator.FlushStatus{
				ElectionState: aggregator.FollowerState,
			},
		}, nil).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instanceMetadata{}, nil, leaderTarget)
	require.Error(t, validator())
}

func TestValidatorForLeaderNoLeaderFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	group := &instanceGroup{
		LeaderID: "instance2",
	}
	instance := instanceMetadata{PlacementInstanceID: "instance1"}
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{
			FlushStatus: aggregator.FlushStatus{
				ElectionState: aggregator.FollowerState,
			},
		}, nil).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instance, group, leaderTarget)
	require.Error(t, validator())
}

func TestValidatorForLeaderFollowerCannotLead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	group := &instanceGroup{
		LeaderID: "instance1",
		All: []instanceMetadata{
			{PlacementInstanceID: "instance1"},
			{PlacementInstanceID: "instance2"},
		},
	}
	instance := instanceMetadata{PlacementInstanceID: "instance1"}
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		Return(aggregator.RuntimeStatus{
			FlushStatus: aggregator.FlushStatus{
				ElectionState: aggregator.FollowerState,
				CanLead:       false,
			},
		}, nil).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instance, group, leaderTarget)
	require.Error(t, validator())
}

func TestValidatorForLeaderFollowerSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	group := &instanceGroup{
		LeaderID: "instance1",
		All: []instanceMetadata{
			{PlacementInstanceID: "instance1", APIEndpoint: "instance1:1234/api"},
			{PlacementInstanceID: "instance2", APIEndpoint: "instance2:1234/api"},
		},
	}
	instance := instanceMetadata{
		PlacementInstanceID: "instance1",
		APIEndpoint:         "instance1:1234/api",
	}
	client := NewMockAggregatorClient(ctrl)
	client.EXPECT().
		Status(gomock.Any()).
		DoAndReturn(func(instance string) (aggregator.RuntimeStatus, error) {
			if instance == "instance1:1234/api" {
				return aggregator.RuntimeStatus{
					FlushStatus: aggregator.FlushStatus{
						ElectionState: aggregator.LeaderState,
						CanLead:       true,
					},
				}, nil
			}
			return aggregator.RuntimeStatus{
				FlushStatus: aggregator.FlushStatus{
					ElectionState: aggregator.FollowerState,
					CanLead:       true,
				},
			}, nil
		}).
		AnyTimes()

	workers := sync.NewWorkerPool(2)
	workers.Init()
	f := newValidatorFactory(client, workers)
	validator := f.ValidatorFor(instance, group, leaderTarget)
	require.NoError(t, validator())
}
