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

package namespace

import (
	"testing"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/stretchr/testify/require"
)

func TestLoadSchemaHistory(t *testing.T) {
	testSchemaOptions := GenTestSchemaOptions("schematest")
	testSchemaReg, err := LoadSchemaHistory(testSchemaOptions)
	require.NoError(t, err)

	testSchema, found := testSchemaReg.Get("first")
	require.True(t, found)
	require.NotNil(t, testSchema)
	require.EqualValues(t, testSchemaOptions.DefaultMessageName, testSchema.Get().GetFullyQualifiedName())

	latestSchema, found := testSchemaReg.GetLatest()
	require.True(t, found)
	require.NotNil(t, latestSchema)
	require.EqualValues(t, testSchemaOptions.DefaultMessageName, latestSchema.Get().GetFullyQualifiedName())
}

func TestParseProto(t *testing.T) {
	out, err := parseProto("mainpkg/main.proto", "schematest")
	require.NoError(t, err)
	require.Len(t, out, 3)
	for _, o := range out {
		t.Log(o.GetFullyQualifiedName())
	}
	require.NotNil(t, out[0].FindMessage("mainpkg.ImportedMessage"))
	require.NotNil(t, out[1].FindMessage("otherpkg.MessageFromOtherPkg"))
	require.NotNil(t, out[2].FindMessage("mainpkg.NestedMessage"))
	require.NotNil(t, out[2].FindMessage("mainpkg.TestMessage"))
}

func TestDistinctIndirectDependency(t *testing.T) {
	out, err := parseProto("deduppkg/main.proto", "schematest")
	require.NoError(t, err)
	require.Len(t, out, 4)
	for _, o := range out {
		t.Log(o.GetFullyQualifiedName())
	}
	require.NotNil(t, out[0].FindMessage("otherpkg.MessageFromOtherPkg"))
	require.NotNil(t, out[1].FindMessage("deduppkg.IndirectMessage"))
	require.NotNil(t, out[2].FindMessage("deduppkg.ImportedMessage"))
	require.NotNil(t, out[3].FindMessage("deduppkg.TestMessage"))

	// test it can be loaded back correctly
	dlist, _ := marshalFileDescriptors(out)

	schemaOpt := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "deduppkg.TestMessage",
	}
	_, err = LoadSchemaHistory(schemaOpt)
	require.NoError(t, err)
}

func TestInvalidSchemaOptions(t *testing.T) {
	out, _ := parseProto("mainpkg/main.proto", "schematest")

	dlist, _ := marshalFileDescriptors(out)

	// missing dependency
	schemaOpt1 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist[1:]},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	_, err := LoadSchemaHistory(schemaOpt1)
	require.Error(t, err)

	// wrong message name
	schemaOpt2 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "WrongMessage",
	}
	_, err = LoadSchemaHistory(schemaOpt2)
	require.Error(t, err)
	require.Equal(t, errInvalidSchemaOptions, xerrors.InnerError(err))

	// reverse the list (so that it is not topologically sorted)
	dlistR := make([][]byte, len(dlist))
	for i := 0; i < len(dlist); i++ {
		dlistR[i] = dlist[len(dlist)-i-1]
	}

	schemaOpt3 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlistR},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	_, err = LoadSchemaHistory(schemaOpt3)
	require.Error(t, err)
}

func TestParseNotProto3(t *testing.T) {
	_, err := parseProto("mainpkg/notproto3.proto", "schematest")
	require.Error(t, err)
	require.Equal(t, errSyntaxNotProto3, xerrors.InnerError(err))
}

func TestSchemaHistorySortedDescending(t *testing.T) {
	out, _ := parseProto("mainpkg/main.proto", "schematest")

	dlist, _ := marshalFileDescriptors(out)
	schemaOpt := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "third", PrevId: "second", Descriptors: dlist},
				{DeployId: "second", PrevId: "first", Descriptors: dlist},
				{DeployId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	_, err := LoadSchemaHistory(schemaOpt)
	require.Error(t, err)
	require.Equal(t, errInvalidSchemaOptions, xerrors.InnerError(err))
}

func TestSchemaOptionsLineageBroken(t *testing.T) {
	out, _ := parseProto("mainpkg/main.proto", "schematest")

	dlist, _ := marshalFileDescriptors(out)
	schemaOpt := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
				{DeployId: "third", PrevId: "second", Descriptors: dlist},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	_, err := LoadSchemaHistory(schemaOpt)
	require.Error(t, err)
	require.Equal(t, errInvalidSchemaOptions, xerrors.InnerError(err))
}

func TestSchemaHistoryCheckLineage(t *testing.T) {
	out, _ := parseProto("mainpkg/main.proto", "schematest")

	dlist, _ := marshalFileDescriptors(out)

	schemaOpt1 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	sr1, err := LoadSchemaHistory(schemaOpt1)
	require.NoError(t, err)

	schemaOpt2 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
				{DeployId: "second", PrevId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	sr2, err := LoadSchemaHistory(schemaOpt2)
	require.NoError(t, err)

	require.True(t, sr1.Extends(emptySchemaHistory()))
	require.True(t, sr2.Extends(emptySchemaHistory()))
	require.False(t, sr1.Extends(sr2))
	require.True(t, sr2.Extends(sr1))

	schemaOpt3 := &nsproto.SchemaOptions{
		History: &nsproto.SchemaHistory{
			Versions: []*nsproto.FileDescriptorSet{
				{DeployId: "first", Descriptors: dlist},
				{DeployId: "third", PrevId: "first", Descriptors: dlist},
			},
		},
		DefaultMessageName: "mainpkg.TestMessage",
	}
	sr3, err := LoadSchemaHistory(schemaOpt3)
	require.NoError(t, err)

	require.True(t, sr3.Extends(sr1))
	require.False(t, sr3.Extends(sr2))
}
