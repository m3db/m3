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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/namespace/kvadmin"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSchemaYaml = `
name: testNamespace
msgName: mainpkg.TestMessage
protoName: "mainpkg/test.proto"
protoMap:
  "mainpkg/test.proto": 'syntax = "proto3";

package mainpkg;

import "mainpkg/imported.proto";

message TestMessage {
  double latitude = 1;
  double longitude = 2;
  int64 epoch = 3;
  bytes deliveryID = 4;
  map<string, string> attributes = 5;
  ImportedMessage an_imported_message = 6;
}
'
  "mainpkg/imported.proto": 'syntax = "proto3";

package mainpkg;

message ImportedMessage {
  double latitude = 1;
  double longitude = 2;
  int64 epoch = 3;
  bytes deliveryID = 4;
}
'
`
)

func TestSchemaDeploy_KVKeyNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	addHandler := NewSchemaHandler(mockClient)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Error case where required fields are not set
	w := httptest.NewRecorder()

	yamlInput := `
        name: testNamespace
    `

	req := httptest.NewRequest("POST", "/schema", strings.NewReader(yamlInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	addHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"namespace is not found\"}\n", string(body))
}

func TestSchemaDeploy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	schemaHandler := NewSchemaHandler(mockClient)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	mockAdminSvc := kvadmin.NewMockNamespaceMetadataAdminService(ctrl)
	newAdminService = func(kv.Store, string, func() string) kvadmin.NamespaceMetadataAdminService { return mockAdminSvc }
	defer func() { newAdminService = kvadmin.NewAdminService }()

	mockAdminSvc.EXPECT().DeploySchema("testNamespace", "mainpkg/test.proto",
		"mainpkg.TestMessage", gomock.Any()).Do(
		func(name, file, msg string, protos map[string]string) {
			schemaOpt, err := namespace.AppendSchemaOptions(nil, file, msg, protos, "first")
			require.NoError(t, err)
			require.Equal(t, "mainpkg.TestMessage", schemaOpt.DefaultMessageName)
			sh, err := namespace.LoadSchemaHistory(schemaOpt)
			require.NoError(t, err)
			descr, ok := sh.GetLatest()
			require.True(t, ok)
			require.NotNil(t, descr)
			require.Equal(t, "first", descr.DeployId())
		}).Return("first", nil)

	w := httptest.NewRecorder()

	req := httptest.NewRequest("POST", "/schema", strings.NewReader(testSchemaYaml))
	require.NotNil(t, req)

	schemaHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"deployID\":\"first\"}", string(body))
}

func TestSchemaDeploy_NamespaceNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	schemaHandler := NewSchemaHandler(mockClient)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	yamlInput := `
        name: "ns-not-found"
    `

	// Ensure adding to an non-existing namespace returns 404
	req := httptest.NewRequest("POST", "/namespace", strings.NewReader(yamlInput))
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
	schemaHandler.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"namespace is not found\"}\n", string(body))
}
