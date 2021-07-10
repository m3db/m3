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

// NewTestHandlerOptsAndClient returns a new test handler
func NewTestHandlerOptsAndClient(
	t *testing.T,
) (
	placementhandler.HandlerOptions,
	*kv.MockStore,
	*clusterclient.MockClient,
) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := clusterclient.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)

	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	mockPlacement := clusterplacement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Proto().Return(placementProto, nil).AnyTimes()
	mockPlacement.EXPECT().Version().Return(0).AnyTimes()

	mockPlacementService := clusterplacement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil).AnyTimes()
	mockPlacementService.EXPECT().Placement().Return(mockPlacement, nil).AnyTimes()

	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()
	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).AnyTimes()

	handlerOpts, err := placementhandler.NewHandlerOptions(
		mockClient, clusterplacement.Configuration{}, nil, instrument.NewOptions())
	require.NoError(t, err)

	return handlerOpts, mockKV, mockClient
}
