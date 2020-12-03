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

package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadyHandlerHealthy(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)
	session.EXPECT().ReadClusterAvailability().Return(true, nil)
	session.EXPECT().WriteClusterAvailability().Return(true, nil)

	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("test-ns"),
		Session:     session,
		Retention:   24 * time.Hour,
	})
	require.NoError(t, err)

	opts := options.EmptyHandlerOptions().SetClusters(clusters)
	readyHandler := NewReadyHandler(opts)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(ReadyHTTPMethod, ReadyURL, nil)

	readyHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `{
		"readyReads": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		],
		"readyWrites": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		]
	  }`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestReadyHandlerUnhealthy(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)
	session.EXPECT().ReadClusterAvailability().Return(true, nil)
	session.EXPECT().WriteClusterAvailability().Return(false, nil)

	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("test-ns"),
		Session:     session,
		Retention:   24 * time.Hour,
	})
	require.NoError(t, err)

	opts := options.EmptyHandlerOptions().SetClusters(clusters)
	readyHandler := NewReadyHandler(opts)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(ReadyHTTPMethod, ReadyURL, nil)

	readyHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	expectedResponse := `{
		"readyReads": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		],
		"notReadyWrites": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		]
	  }`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestReadyHandlerOnlyReads(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)
	session.EXPECT().ReadClusterAvailability().Return(true, nil)
	session.EXPECT().WriteClusterAvailability().Return(false, nil)

	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("test-ns"),
		Session:     session,
		Retention:   24 * time.Hour,
	})
	require.NoError(t, err)

	opts := options.EmptyHandlerOptions().SetClusters(clusters)
	readyHandler := NewReadyHandler(opts)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(ReadyHTTPMethod, ReadyURL+"?writes=false", nil)

	readyHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `{
		"readyReads": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		],
		"notReadyWrites": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		]
	  }`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestReadyHandlerOnlyWrites(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)
	session.EXPECT().ReadClusterAvailability().Return(false, nil)
	session.EXPECT().WriteClusterAvailability().Return(true, nil)

	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("test-ns"),
		Session:     session,
		Retention:   24 * time.Hour,
	})
	require.NoError(t, err)

	opts := options.EmptyHandlerOptions().SetClusters(clusters)
	readyHandler := NewReadyHandler(opts)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(ReadyHTTPMethod, ReadyURL+"?reads=false", nil)

	readyHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `{
		"notReadyReads": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		],
		"readyWrites": [
		  {
			"attributes": {
			  "metricsType": "unaggregated",
			  "resolution": "0s",
			  "retention": "24h0m0s"
			},
			"id": "test-ns"
		  }
		]
	  }`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}
