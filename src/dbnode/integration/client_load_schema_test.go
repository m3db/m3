// +build integration

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

package integration

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	testProtoStr = `syntax = "proto3";

package mainpkg;

message TestMessage {
  double latitude = 1;
  double longitude = 2;
  int64 epoch = 3;
  bytes deliveryID = 4;
  map<string, string> attributes = 5;
}
`
)

func TestClientLoadSchemaFromFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := ioutil.TempDir("", "m3db-client-schema")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	protoFile := filepath.Join(tempDir, "test.proto")
	require.NoError(t, ioutil.WriteFile(protoFile, []byte(testProtoStr), 0644))

	schemaReg := namespace.NewSchemaRegistry(true, nil)
	protoMsg := "mainpkg.TestMessage"
	require.NoError(t, namespace.LoadSchemaRegistryFromFile(schemaReg, ident.StringID("ns1"), "fromconfig", protoFile, protoMsg))
	expectedDescr, err := schemaReg.GetLatestSchema(ident.StringID("ns1"))
	require.NoError(t, err)
	require.EqualValues(t, protoMsg, expectedDescr.Get().GetFullyQualifiedName())

	cfg := &client.Configuration{
		Proto: &client.ProtoConfiguration{
			Enabled: true,
			SchemaRegistry: map[string]client.NamespaceProtoSchema{
				"ns1": {MessageName: protoMsg, SchemaFilePath: protoFile},
			},
		},
	}

	mockTopo := topology.NewMockInitializer(ctrl)
	adminClient, err := cfg.NewAdminClient(client.ConfigurationParameters{TopologyInitializer: mockTopo})
	require.NoError(t, err)

	descr, err := adminClient.
		Options().
		SchemaRegistry().
		GetLatestSchema(ident.StringID("ns1"))
	require.NoError(t, err)
	require.NotNil(t, descr)
	require.EqualValues(t, expectedDescr.String(), descr.String())
}
