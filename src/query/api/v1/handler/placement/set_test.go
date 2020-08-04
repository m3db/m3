// Copyright (c) 2019 Uber Technologies, Inc.
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

package placement

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/jsonpb"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	setExistingTestPlacementProto = &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
			},
		},
	}
	setNewTestPlacementProto = &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
			},
			"host2": &placementpb.Instance{
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
	setTestPlacementReqProto = &admin.PlacementSetRequest{
		Placement: setNewTestPlacementProto,
		Version:   0,
		Confirm:   true,
	}
)

func TestPlacementSetHandler(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		var url string
		switch serviceName {
		case handleroptions.M3DBServiceName:
			url = M3DBSetURL
		case handleroptions.M3AggregatorServiceName:
			url = M3AggSetURL
		case handleroptions.M3CoordinatorServiceName:
			url = M3CoordinatorSetURL
		default:
			require.FailNow(t, "unexpected service name")
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewSetHandler(handlerOpts)

		// Test placement init success
		reqBody, err := (&jsonpb.Marshaler{}).MarshalToString(setTestPlacementReqProto)
		require.NoError(t, err)

		req := httptest.NewRequest(SetHTTPMethod, url, strings.NewReader(reqBody))
		require.NotNil(t, req)

		existingPlacement, err := placement.NewPlacementFromProto(setExistingTestPlacementProto)
		require.NoError(t, err)

		mockPlacementService.EXPECT().
			Placement().
			Return(existingPlacement, nil)

		newPlacement, err := placement.NewPlacementFromProto(setNewTestPlacementProto)
		require.NoError(t, err)

		mockPlacementService.EXPECT().
			CheckAndSet(gomock.Any(), gomock.Any()).
			Return(newPlacement, nil)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		w := httptest.NewRecorder()
		handler.ServeHTTP(svcDefaults, w, req)
		resp := w.Result()
		body := w.Body.String()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		expectedBody, err := (&jsonpb.Marshaler{
			EmitDefaults: true,
		}).MarshalToString(&admin.PlacementSetResponse{
			Placement: setNewTestPlacementProto,
			DryRun:    !setTestPlacementReqProto.Confirm,
		})
		require.NoError(t, err)

		expected := xtest.MustPrettyJSONString(t, expectedBody)
		actual := xtest.MustPrettyJSONString(t, body)

		assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
	})
}
