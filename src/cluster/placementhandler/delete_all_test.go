// Copyright (c) 2018 Uber Technologies, Inc.
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

package placementhandler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteAllHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runForAllAllowedServices(func(serviceName string) {
		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, placement.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewDeleteAllHandler(handlerOpts)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		inst0 := placement.NewInstance().
			SetID("foo").
			SetEndpoint("foo:123").
			SetHostname("foo").
			SetPort(123).
			SetIsolationGroup("foo-group").
			SetWeight(1).
			SetZone("default")
		if serviceName == handleroptions.M3AggregatorServiceName {
			inst0 = inst0.SetShardSetID(0)
		}

		inst1 := placement.NewInstance().
			SetID("bar").
			SetEndpoint("bar:123").
			SetHostname("bar").
			SetPort(123).
			SetIsolationGroup("bar-group").
			SetWeight(1).
			SetZone("default")
		if serviceName == handleroptions.M3AggregatorServiceName {
			inst1 = inst1.SetShardSetID(0)
		}

		instances := []placement.Instance{inst0, inst1}

		existing := placement.NewPlacement().
			SetInstances(instances)

		mockKVStore := kv.NewMockStore(ctrl)

		// Test delete success
		w := httptest.NewRecorder()
		req := httptest.NewRequest(DeleteAllHTTPMethod, M3DBDeleteAllURL, nil)
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(existing, nil)
		mockPlacementService.EXPECT().Delete()
		if serviceName == handleroptions.M3AggregatorServiceName {
			flushTimesMgrOpts := aggregator.NewFlushTimesManagerOptions()
			electionMgrOpts := aggregator.NewElectionManagerOptions()
			mockClient.EXPECT().Store(gomock.Any()).Return(mockKVStore, nil)
			mockKVStore.EXPECT().
				Get(gomock.Any()).
				DoAndReturn(func(k string) (kv.Value, error) {
					for _, inst := range instances {
						switch k {
						case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), inst.ShardSetID()):
							return nil, nil
						case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), inst.ShardSetID()):
							return nil, nil
						}
					}
					return nil, errors.New("unexpected")
				}).
				AnyTimes()
			mockKVStore.EXPECT().
				Delete(gomock.Any()).
				DoAndReturn(func(k string) (kv.Value, error) {
					for _, inst := range instances {
						switch k {
						case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), inst.ShardSetID()):
							return nil, nil
						case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), inst.ShardSetID()):
							return nil, nil
						}
					}
					return nil, errors.New("unexpected")
				}).
				AnyTimes()
		}

		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Test delete error
		w = httptest.NewRecorder()
		req = httptest.NewRequest(DeleteAllHTTPMethod, M3DBDeleteAllURL, nil)
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(existing, nil)
		mockPlacementService.EXPECT().Delete().Return(errors.New("error"))
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

		// Test delete not found error
		w = httptest.NewRecorder()
		req = httptest.NewRequest(DeleteAllHTTPMethod, M3DBDeleteAllURL, nil)
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}
