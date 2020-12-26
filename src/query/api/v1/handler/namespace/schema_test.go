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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/namespace/kvadmin"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mainProtoStr = `syntax = "proto3";

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
`
	importedProtoStr = `
syntax = "proto3";

package mainpkg;

message ImportedMessage {
  double latitude = 1;
  double longitude = 2;
  int64 epoch = 3;
  bytes deliveryID = 4;
}
`
	testSchemaJQ = `
{
  name: "testNamespace",
  msgName: "mainpkg.TestMessage",
  protoName: "mainpkg/test.proto",
  protoMap:
    {
      "mainpkg/test.proto": $file1,
      "mainpkg/imported.proto": $file2
    }
}
`
	testSchemaJSON = `
        {
          "name": "testNamespace",
          "msgName": "mainpkg.TestMessage",
          "protoName": "mainpkg/test.proto",
          "protoMap": {
            "mainpkg/test.proto": "syntax = \"proto3\";\n\npackage mainpkg;\n\nimport \"mainpkg/imported.proto\";\n\nmessage TestMessage {\n  double latitude = 1;\n  double longitude = 2;\n  int64 epoch = 3;\n  bytes deliveryID = 4;\n  map<string, string> attributes = 5;\n  ImportedMessage an_imported_message = 6;\n}",
            "mainpkg/imported.proto": "\nsyntax = \"proto3\";\n\npackage mainpkg;\n\nmessage ImportedMessage {\n  double latitude = 1;\n  double longitude = 2;\n  int64 epoch = 3;\n  bytes deliveryID = 4;\n}"
          }
        }
`
)

var (
	svcDefaults = handleroptions.ServiceNameAndDefaults{
		ServiceName: "m3db",
	}
)

func genTestJSON(t *testing.T) string {
	tempDir, err := ioutil.TempDir("", "schema_deploy_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	file1 := filepath.Join(tempDir, "test.proto")
	file2 := filepath.Join(tempDir, "imported.proto")
	require.NoError(t, ioutil.WriteFile(file1, []byte(mainProtoStr), 0644))
	require.NoError(t, ioutil.WriteFile(file2, []byte(importedProtoStr), 0644))

	jqfile := filepath.Join(tempDir, "test.jq")
	require.NoError(t, ioutil.WriteFile(jqfile, []byte(testSchemaJQ), 0644))

	file1var := "\"$(<" + file1 + ")\""
	file2var := "\"$(<" + file2 + ")\""
	cmd := exec.Command("sh", "-c", "jq -n --arg file1 "+file1var+
		" --arg file2 "+file2var+" -f "+jqfile)
	t.Logf("cmd args are %v\n", cmd.Args)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err)
	t.Logf("generated json is \n%s\n", string(out))
	return string(out)
}

func TestGenTestJson(t *testing.T) {
	t.Skip("skip for now as jq is not installed on buildkite")
	genTestJSON(t)
}

func TestSchemaDeploy_KVKeyNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	addHandler := NewSchemaHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	// Error case where required fields are not set
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name": "testNamespace",
	}

	req := httptest.NewRequest("POST", "/schema",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	addHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.JSONEq(t, `{"status":"error","error":"namespace is not found"}`, string(body))
}

func TestSchemaDeploy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	schemaHandler := NewSchemaHandler(mockClient, instrument.NewOptions())
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

	// TODO [haijun] buildkite does not have jq, so use the pre-generated json string.
	//testSchemaJson := genTestJson(t)
	req := httptest.NewRequest("POST", "/schema", strings.NewReader(testSchemaJSON))
	require.NotNil(t, req)

	schemaHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"deployID\":\"first\"}", string(body))
}

func TestSchemaDeploy_NamespaceNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	schemaHandler := NewSchemaHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	jsonInput := xjson.Map{
		"name": "no-such-namespace",
	}

	// Ensure adding to an non-existing namespace returns 404
	req := httptest.NewRequest("POST", "/namespace",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testNamespace": {
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
	schemaHandler.ServeHTTP(svcDefaults, w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.JSONEq(t, `{"status":"error","error":"namespace is not found"}`, string(body))
}

func TestSchemaReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV := setupNamespaceTest(t, ctrl)
	schemaHandler := NewSchemaResetHandler(mockClient, instrument.NewOptions())
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)

	mockAdminSvc := kvadmin.NewMockNamespaceMetadataAdminService(ctrl)
	newAdminService = func(kv.Store, string, func() string) kvadmin.NamespaceMetadataAdminService { return mockAdminSvc }
	defer func() { newAdminService = kvadmin.NewAdminService }()

	mockAdminSvc.EXPECT().ResetSchema("testNamespace").Return(nil)

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"name": "testNamespace",
	}

	req := httptest.NewRequest("DELETE", "/schema",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	schemaHandler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		fmt.Sprintf("response: %s", body))

	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/schema",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)
	req.Header.Add("Force", "true")

	schemaHandler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		fmt.Sprintf("response: %s", body))
	assert.Equal(t, "{}", string(body))
}
