// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testUpdateJSON = `
{
		"name": "testNamespace",
		"options": {
			"retentionOptions": {
				"retentionPeriodDuration": "96h"
			},
			"runtimeOptions": {
				"writeIndexingPerCPUConcurrency": 16
			},
			"aggregationOptions": {
				"aggregations": [
					{
						"aggregated": true,
						"attributes": {
							"resolutionDuration": "5m"
						}
					}
				]
			}
		}
}
`

	testUpdateJSONNop = `
{
		"name": "testNamespace",
		"options": {
			"retentionOptions": {}
		}
}
`
)

func TestNamespaceUpdateHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	updateHandler := NewUpdateHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).Times(2)

	// Error case where required fields are not set
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name":    "testNamespace",
		"options": xjson.Map{},
	}

	req := httptest.NewRequest("POST", "/namespace",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	updateHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.JSONEq(t,
		`{"status":"error","error":"unable to validate update request: update options cannot be empty"}`,
		string(body))

	// Test good case. Note: there is no way to tell the difference between a boolean
	// being false and it not being set by a user.
	w = httptest.NewRecorder()

	req = httptest.NewRequest("PUT", "/namespace", strings.NewReader(testUpdateJSON))
	require.NotNil(t, req)

	extendedOpts, err := xtest.NewExtendedOptionsProto("foo")
	require.NoError(t, err)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testNamespace": {
				BootstrapEnabled:      true,
				CacheBlocksOnRetrieve: &types.BoolValue{Value: true},
				FlushEnabled:          true,
				SnapshotEnabled:       true,
				WritesToCommitLog:     true,
				CleanupEnabled:        false,
				RepairEnabled:         false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     172800000000000,
					BlockSizeNanos:                           7200000000000,
					BufferFutureNanos:                        600000000000,
					BufferPastNanos:                          600000000000,
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: 3600000000000,
				},
				ExtendedOptions: extendedOpts,
			},
		},
	}

	mockValue := kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)
	mockValue.EXPECT().Version().Return(0)
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)

	mockKV.EXPECT().CheckAndSet(M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)
	updateHandler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expected := xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"registry": xjson.Map{
				"namespaces": xjson.Map{
					"testNamespace": xjson.Map{
						"aggregationOptions": xjson.Map{
							"aggregations": xjson.Array{
								xjson.Map{
									"aggregated": true,
									"attributes": xjson.Map{
										"resolutionNanos": "300000000000",
										"downsampleOptions": xjson.Map{
											"all": true,
										},
									},
								},
							},
						},
						"bootstrapEnabled":      true,
						"cacheBlocksOnRetrieve": true,
						"flushEnabled":          true,
						"writesToCommitLog":     true,
						"cleanupEnabled":        false,
						"repairEnabled":         false,
						"retentionOptions": xjson.Map{
							"retentionPeriodNanos":                     "345600000000000",
							"blockSizeNanos":                           "7200000000000",
							"bufferFutureNanos":                        "600000000000",
							"bufferPastNanos":                          "600000000000",
							"blockDataExpiry":                          true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "3600000000000",
							"futureRetentionPeriodNanos":               "0",
						},
						"snapshotEnabled": true,
						"indexOptions": xjson.Map{
							"enabled":        false,
							"blockSizeNanos": "7200000000000",
						},
						"runtimeOptions": xjson.Map{
							"flushIndexingPerCPUConcurrency": nil,
							"writeIndexingPerCPUConcurrency": 16,
						},
						"schemaOptions":     nil,
						"stagingState":      xjson.Map{"status": "UNKNOWN"},
						"coldWritesEnabled": false,
						"extendedOptions":   xtest.NewExtendedOptionsJson("foo"),
					},
				},
			},
		})

	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual,
		xtest.Diff(expected, actual))

	// Ensure an empty request respects existing namespaces.
	w = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/namespace", strings.NewReader(testUpdateJSONNop))
	require.NotNil(t, req)

	mockValue = kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)
	mockValue.EXPECT().Version().Return(0)
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)

	mockKV.EXPECT().CheckAndSet(M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)
	updateHandler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expected = xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"registry": xjson.Map{
				"namespaces": xjson.Map{
					"testNamespace": xjson.Map{
						"aggregationOptions":    nil,
						"bootstrapEnabled":      true,
						"cacheBlocksOnRetrieve": true,
						"flushEnabled":          true,
						"writesToCommitLog":     true,
						"cleanupEnabled":        false,
						"repairEnabled":         false,
						"retentionOptions": xjson.Map{
							"retentionPeriodNanos":                     "172800000000000",
							"blockSizeNanos":                           "7200000000000",
							"bufferFutureNanos":                        "600000000000",
							"bufferPastNanos":                          "600000000000",
							"blockDataExpiry":                          true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "3600000000000",
							"futureRetentionPeriodNanos":               "0",
						},
						"snapshotEnabled": true,
						"indexOptions": xjson.Map{
							"enabled":        false,
							"blockSizeNanos": "7200000000000",
						},
						"runtimeOptions":    nil,
						"schemaOptions":     nil,
						"stagingState":      xjson.Map{"status": "UNKNOWN"},
						"coldWritesEnabled": false,
						"extendedOptions":   xtest.NewExtendedOptionsJson("foo"),
					},
				},
			},
		})

	actual = xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual,
		xtest.Diff(expected, actual))
}

func TestValidateUpdateRequest(t *testing.T) {
	var (
		reqEmptyName = &admin.NamespaceUpdateRequest{
			Options: &nsproto.NamespaceOptions{
				BootstrapEnabled: true,
			},
		}

		reqEmptyOptions = &admin.NamespaceUpdateRequest{
			Name: "foo",
		}

		reqNoNonZeroFields = &admin.NamespaceUpdateRequest{
			Name:    "foo",
			Options: &nsproto.NamespaceOptions{},
		}

		reqNonZeroBootstrap = &admin.NamespaceUpdateRequest{
			Name: "foo",
			Options: &nsproto.NamespaceOptions{
				RetentionOptions: &nsproto.RetentionOptions{
					BlockSizeNanos: 1,
				},
				BootstrapEnabled: true,
			},
		}

		reqNonZeroBlockSize = &admin.NamespaceUpdateRequest{
			Name: "foo",
			Options: &nsproto.NamespaceOptions{
				RetentionOptions: &nsproto.RetentionOptions{
					BlockSizeNanos: 1,
				},
			},
		}

		reqValid = &admin.NamespaceUpdateRequest{
			Name: "foo",
			Options: &nsproto.NamespaceOptions{
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos: 1,
				},
			},
		}
	)

	for _, test := range []struct {
		name    string
		request *admin.NamespaceUpdateRequest
		expErr  error
	}{
		{
			name:    "emptyName",
			request: reqEmptyName,
			expErr:  errEmptyNamespaceName,
		},
		{
			name:    "emptyOptions",
			request: reqEmptyOptions,
			expErr:  errEmptyNamespaceOptions,
		},
		{
			name:    "emptyNoNonZeroFields",
			request: reqNoNonZeroFields,
			expErr:  errEmptyNamespaceOptions,
		},
		{
			name:    "nonZeroBootstrapField",
			request: reqNonZeroBootstrap,
			expErr:  errNamespaceFieldImmutable,
		},
		{
			name:    "nonZeroBlockSize",
			request: reqNonZeroBlockSize,
			expErr:  errNamespaceFieldImmutable,
		},
		{
			name:    "valid",
			request: reqValid,
			expErr:  nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateUpdateRequest(test.request)
			if err != nil {
				assert.True(t, errors.Is(err, test.expErr),
					fmt.Sprintf("expected=%s, actual=%s", test.expErr, err))
				return
			}

			assert.NoError(t, err)
		})
	}
}
