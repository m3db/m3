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
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAddJSON = `
{
		"name": "testNamespace",
		"options": {
			"bootstrapEnabled": true,
			"flushEnabled": true,
			"writesToCommitLog": true,
			"cleanupEnabled": true,
			"repairEnabled": true,
			"retentionOptions": {
				"retentionPeriodNanos": 172800000000000,
				"blockSizeNanos": 7200000000000,
				"bufferFutureNanos": 600000000000,
				"bufferPastNanos": 600000000000,
				"blockDataExpiry": true,
				"blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
			},
			"snapshotEnabled": true,
			"indexOptions": {
				"enabled": true,
				"blockSizeNanos": 7200000000000
			}
		}
}
`

func TestNamespaceAddHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	addHandler := NewAddHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Error case where required fields are not set
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name":    "testNamespace",
		"options": xjson.Map{},
	}

	req := httptest.NewRequest("POST", "/namespace",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	addHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"unable to get metadata: retention options must be set\"}\n", string(body))

	// Test good case. Note: there is no way to tell the difference between a boolean
	// being false and it not being set by a user.
	w = httptest.NewRecorder()

	req = httptest.NewRequest("POST", "/namespace", strings.NewReader(testAddJSON))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	mockKV.EXPECT().CheckAndSet(M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)
	addHandler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{\"testNamespace\":{\"bootstrapEnabled\":true,\"flushEnabled\":true,\"writesToCommitLog\":true,\"cleanupEnabled\":true,\"repairEnabled\":true,\"retentionOptions\":{\"retentionPeriodNanos\":\"172800000000000\",\"blockSizeNanos\":\"7200000000000\",\"bufferFutureNanos\":\"600000000000\",\"bufferPastNanos\":\"600000000000\",\"blockDataExpiry\":true,\"blockDataExpiryAfterNotAccessPeriodNanos\":\"300000000000\",\"futureRetentionPeriodNanos\":\"0\"},\"snapshotEnabled\":true,\"indexOptions\":{\"enabled\":true,\"blockSizeNanos\":\"7200000000000\"},\"schemaOptions\":null,\"coldWritesEnabled\":false}}}}", string(body))
}

func TestNamespaceAddHandler_Conflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	addHandler := NewAddHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Ensure adding an existing namespace returns 409
	req := httptest.NewRequest("POST", "/namespace", strings.NewReader(testAddJSON))
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testNamespace": &nsproto.NamespaceOptions{
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
			},
		},
	}

	mockValue := kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)
	mockValue.EXPECT().Version().Return(0)
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)

	w := httptest.NewRecorder()
	addHandler.ServeHTTP(svcDefaults, w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}
