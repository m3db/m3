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

package namespace

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNamespaceTest(t *testing.T, ctrl *gomock.Controller) (*client.MockClient, *kv.MockStore) {
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)

	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()

	return mockClient, mockKV
}

func TestNamespaceGetHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	getHandler := NewGetHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Test no namespace
	w := httptest.NewRecorder()

	req := httptest.NewRequest("GET", "/namespace/get", nil)
	require.NotNil(t, req)

	matcher := newStoreOptionsMatcher("", "", "test_env")
	mockClient.EXPECT().Store(matcher).Return(mockKV, nil)
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	getHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{}}}", string(body))

	// Test namespace present
	w = httptest.NewRecorder()

	req = httptest.NewRequest("GET", "/namespace/get", nil)
	req.Header.Set(headers.HeaderClusterEnvironmentName, "test_env")
	require.NotNil(t, req)

	extendedOpts := xtest.NewTestExtendedOptionsProto("foo")

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"test": {
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				SnapshotEnabled:   true,
				WritesToCommitLog: true,
				CleanupEnabled:    false,
				RepairEnabled:     false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     172800000000000,
					BlockSizeNanos:                           7200000000000,
					BufferFutureNanos:                        600000000000,
					BufferPastNanos:                          600000000000,
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: 3600000000000,
				},
				ExtendedOptions: extendedOpts,
				StagingState:    &nsproto.StagingState{Status: nsproto.StagingStatus_READY},
			},
		},
	}

	mockValue := kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)
	getHandler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expected := xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"registry": xjson.Map{
				"namespaces": xjson.Map{
					"test": xjson.Map{
						"aggregationOptions":    nil,
						"bootstrapEnabled":      true,
						"cacheBlocksOnRetrieve": nil,
						"cleanupEnabled":        false,
						"coldWritesEnabled":     false,
						"flushEnabled":          true,
						"indexOptions":          nil,
						"repairEnabled":         false,
						"retentionOptions": xjson.Map{
							"blockDataExpiry":                          true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "3600000000000",
							"blockSizeNanos":                           "7200000000000",
							"bufferFutureNanos":                        "600000000000",
							"bufferPastNanos":                          "600000000000",
							"futureRetentionPeriodNanos":               "0",
							"retentionPeriodNanos":                     "172800000000000",
						},
						"runtimeOptions":    nil,
						"schemaOptions":     nil,
						"snapshotEnabled":   true,
						"stagingState":      xjson.Map{"status": "READY"},
						"writesToCommitLog": true,
						"extendedOptions":   xtest.NewTestExtendedOptionsJSON("foo"),
					},
				},
			},
		})

	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual,
		xtest.Diff(expected, actual))
}

func TestNamespaceGetHandlerWithDebug(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	getHandler := NewGetHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Test namespace present
	w := httptest.NewRecorder()

	req := httptest.NewRequest("GET", "/namespace/get?debug=true", nil)
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"test": {
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				SnapshotEnabled:   true,
				WritesToCommitLog: true,
				CleanupEnabled:    false,
				RepairEnabled:     false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     172800000000000,
					BlockSizeNanos:                           7200000000000,
					BufferFutureNanos:                        600000000000,
					BufferPastNanos:                          600000000000,
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: 3600000000000,
				},
				StagingState: &nsproto.StagingState{Status: nsproto.StagingStatus_UNKNOWN},
			},
		},
	}

	mockValue := kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)
	getHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expected := xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"registry": xjson.Map{
				"namespaces": xjson.Map{
					"test": xjson.Map{
						"aggregationOptions":    nil,
						"bootstrapEnabled":      true,
						"cacheBlocksOnRetrieve": nil,
						"cleanupEnabled":        false,
						"coldWritesEnabled":     false,
						"flushEnabled":          true,
						"indexOptions":          nil,
						"repairEnabled":         false,
						"retentionOptions": xjson.Map{
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodDuration": "1h0m0s",
							"blockSizeDuration":                           "2h0m0s",
							"bufferFutureDuration":                        "10m0s",
							"bufferPastDuration":                          "10m0s",
							"futureRetentionPeriodDuration":               "0s",
							"retentionPeriodDuration":                     "48h0m0s",
						},
						"runtimeOptions":    nil,
						"schemaOptions":     nil,
						"stagingState":      xjson.Map{"status": "UNKNOWN"},
						"snapshotEnabled":   true,
						"writesToCommitLog": true,
						"extendedOptions":   nil,
					},
				},
			},
		})

	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual,
		xtest.Diff(expected, actual))
}
