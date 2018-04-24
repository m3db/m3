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

package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/kv"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceAddHandler(t *testing.T) {
	mockClient, mockKV, _ := SetupNamespaceTest(t)
	handler := NewNamespaceAddHandler(mockClient)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"name": "testNamespace",
			"retention_period": "48h",
			"block_size": "2h",
			"buffer_future": "10m",
			"buffer_past": "5m",
			"needs_fileset_cleanup": false
		}
	`

	req := httptest.NewRequest("POST", "/namespace/add", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	mockKV.EXPECT().Set(M3DBNodeNamespacesKey, gomock.Not(nil)).Return(1, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{\"testNamespace\":{\"needsBootstrap\":false,\"needsFlush\":false,\"writesToCommitLog\":false,\"needsFilesetCleanup\":false,\"needsRepair\":false,\"retentionOptions\":{\"retentionPeriodNanos\":\"172800000000000\",\"blockSizeNanos\":\"7200000000000\",\"bufferFutureNanos\":\"600000000000\",\"bufferPastNanos\":\"300000000000\",\"blockDataExpiry\":false,\"blockDataExpiryAfterNotAccessPeriodNanos\":\"300000000000\"}}}}}", string(body))
}
