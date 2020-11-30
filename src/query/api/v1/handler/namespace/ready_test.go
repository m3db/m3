// Copyright (c) 2020  Uber Technologies, Inc.
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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNamespaceReadyHandler(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, nsID := testSetup(t, ctrl)
	mockKV.EXPECT().CheckAndSet(M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	mockSession := client.NewMockSession(ctrl)
	testClusterNs := testClusterNamespace{
		session: mockSession,
		id:      nsID,
		options: m3.ClusterNamespaceOptions{},
	}
	testClusters := testClusters{
		nonReadyNamespaces: []m3.ClusterNamespace{&testClusterNs},
	}
	mockSession.EXPECT().FetchTaggedIDs(testClusterNs.NamespaceID(), index.Query{Query: idx.NewAllQuery()},
		index.QueryOptions{SeriesLimit: 1, DocsLimit: 1}).Return(nil, client.FetchResponseMetadata{}, nil)

	readyHandler := NewReadyHandler(mockClient, &testClusters, instrument.NewOptions())

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name": "testNamespace",
	}

	req := httptest.NewRequest("POST", "/namespace/ready",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	readyHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	expected := xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"ready": true,
		})
	actual := xtest.MustPrettyJSONString(t, string(body))
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestNamespaceReadyHandlerWithForce(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, _ := testSetup(t, ctrl)
	mockKV.EXPECT().CheckAndSet(M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	readyHandler := NewReadyHandler(mockClient, nil, instrument.NewOptions())

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name":  "testNamespace",
		"force": true,
	}

	req := httptest.NewRequest("POST", "/namespace/ready",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	readyHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	expected := xtest.MustPrettyJSONMap(t,
		xjson.Map{
			"ready": true,
		})
	actual := xtest.MustPrettyJSONString(t, string(body))
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestNamespaceReadyFailIfNoClusters(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockClient, _, _ := testSetup(t, ctrl)

	readyHandler := NewReadyHandler(mockClient, nil, instrument.NewOptions())

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name": "testNamespace",
	}

	req := httptest.NewRequest("POST", "/namespace/ready",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	readyHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		fmt.Sprintf("response: %s", w.Body.String()))
}

func TestNamespaceReadyFailIfNamespaceMissing(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockClient, _, _ := testSetup(t, ctrl)

	readyHandler := NewReadyHandler(mockClient, nil, instrument.NewOptions())

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name": "missingNamespace",
	}

	req := httptest.NewRequest("POST", "/namespace/ready",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	readyHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func testSetup(t *testing.T, ctrl *gomock.Controller) (*clusterclient.MockClient, *kv.MockStore, ident.ID) {
	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	nsID := ident.StringID("testNamespace")
	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			nsID.String(): {
				// Required for validation
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
	return mockClient, mockKV, nsID
}

type testClusterNamespace struct {
	session client.Session
	id      ident.ID
	options m3.ClusterNamespaceOptions
}

func (t *testClusterNamespace) NamespaceID() ident.ID {
	return t.id
}

func (t *testClusterNamespace) Options() m3.ClusterNamespaceOptions {
	return t.options
}

func (t *testClusterNamespace) Session() client.Session {
	return t.session
}

type testClusters struct {
	nonReadyNamespaces m3.ClusterNamespaces
}

func (t *testClusters) ClusterNamespaces() m3.ClusterNamespaces {
	panic("implement me")
}

func (t *testClusters) NonReadyClusterNamespaces() m3.ClusterNamespaces {
	return t.nonReadyNamespaces
}

func (t *testClusters) Close() error {
	panic("implement me")
}

func (t *testClusters) UnaggregatedClusterNamespace() (m3.ClusterNamespace, bool) {
	panic("implement me")
}

func (t *testClusters) AggregatedClusterNamespace(attrs m3.RetentionResolution) (m3.ClusterNamespace, bool) {
	panic("implement me")
}
