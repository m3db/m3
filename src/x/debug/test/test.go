// Copyright (c) 2021 Uber Technologies, Inc.
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

// Package debugtest contains test helper methods
package debugtest

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	clusterplacement "github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/x/instrument"
)

// NewTestHandlerOptsAndClient returns a new test handler.
func NewTestHandlerOptsAndClient(
	t *testing.T,
) (
	placementhandler.HandlerOptions,
	*kv.MockStore,
	*clusterclient.MockClient,
) {
	ctrl := gomock.NewController(t)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": {
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
			},
			"host2": {
				Id:             "host2",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host2:1234",
				Hostname:       "host2",
				Port:           1234,
			},
		},
	}

	mockPlacement := clusterplacement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Proto().Return(placementProto, nil).AnyTimes()
	mockPlacement.EXPECT().Version().Return(0).AnyTimes()

	return NewTestHandlerOptsAndClientWithPlacement(t, ctrl, mockPlacement)
}

// NewTestHandlerOptsAndClientWithPlacement returns a new test handler with supplied placement.
func NewTestHandlerOptsAndClientWithPlacement(
	t *testing.T,
	ctrl *gomock.Controller,
	placement clusterplacement.Placement,
) (
	placementhandler.HandlerOptions,
	*kv.MockStore,
	*clusterclient.MockClient,
) {
	mockClient := clusterclient.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)

	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	mockPlacementService := clusterplacement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil).AnyTimes()
	mockPlacementService.EXPECT().Placement().Return(placement, nil).AnyTimes()

	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()
	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).AnyTimes()

	handlerOpts, err := placementhandler.NewHandlerOptions(
		mockClient, clusterplacement.Configuration{}, nil, instrument.NewOptions())
	require.NoError(t, err)

	return handlerOpts, mockKV, mockClient
}
