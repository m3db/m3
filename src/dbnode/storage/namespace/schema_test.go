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

package namespace_test

import (
	"testing"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	testproto "github.com/m3db/m3/src/dbnode/generated/proto/schematest"
	testproto2 "github.com/m3db/m3/src/dbnode/generated/proto/schematest2"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"
	"github.com/stretchr/testify/require"
)

func getTestSchemaOptions() *nsproto.SchemaOptions {
	imported := &testproto.ImportedMessage{}
	importedD, _ := imported.Descriptor()
	otherpkg := &testproto2.MessageFromOtherPkg{}
	otherpkgD, _ := otherpkg.Descriptor()
	main := &testproto.TestMessage{}
	mainD, _ := main.Descriptor()
	return &nsproto.SchemaOptions{
		Repo: &nsproto.FileDescriptorRepo{
			History: []*nsproto.FileDescriptorSet{
				{Version: 1, Descriptors: [][]byte{importedD, otherpkgD, mainD}},
			},
		},
		Schemas: map[string]*nsproto.SchemaMeta{"id1": {MessageName: "TestMessage"}},
	}
}

func TestLoadSchemaRegistry(t *testing.T) {
	testSchemaOptions := getTestSchemaOptions()
	testSchemaReg, err := namespace.LoadSchemaRegistry(testSchemaOptions)
	require.NoError(t, err)

	testSchema, err := testSchemaReg.Get(ident.StringID("id1"))
	require.NoError(t, err)
	require.NotNil(t, testSchema)
	require.EqualValues(t, "TestMessage", testSchema.Get().GetName())
}
