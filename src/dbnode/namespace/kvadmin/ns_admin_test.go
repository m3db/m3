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

package kvadmin

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"
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

	nsRegKey        = "nsRegKey"
	testNamespaceID = "test-namespace"
)

func TestAdminService_DeploySchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })
	require.NotNil(t, as)

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)
	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	protoFile := "mainpkg/test.proto"
	protoMsg := "mainpkg.TestMessage"
	protoMap := map[string]string{protoFile: mainProtoStr, "mainpkg/imported.proto": importedProtoStr}

	expectedSchemaOpt, err := namespace.
		AppendSchemaOptions(nil, protoFile, protoMsg, protoMap, testNamespaceID)
	require.NoError(t, err)
	expectedSh, err := namespace.LoadSchemaHistory(expectedSchemaOpt)
	require.NoError(t, err)
	expectedMeta, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetSchemaHistory(expectedSh))
	require.NoError(t, err)
	expectedMap, err := namespace.NewMap([]namespace.Metadata{expectedMeta})
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().CheckAndSet(nsRegKey, 1, gomock.Any()).Return(2, nil).Do(
		func(k string, version int, actualReg *nsproto.Registry) {
			actualMap, err := namespace.FromProto(*actualReg)
			require.NoError(t, err)
			require.NotEmpty(t, actualMap)
			require.True(t, actualMap.Equal(expectedMap))
		})
	_, err = as.DeploySchema("ns1", protoFile, protoMsg, protoMap)
	require.NoError(t, err)
}

func TestAdminService_ResetSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })
	require.NotNil(t, as)

	protoFile := "mainpkg/test.proto"
	protoMsg := "mainpkg.TestMessage"
	protoMap := map[string]string{protoFile: mainProtoStr, "mainpkg/imported.proto": importedProtoStr}
	currentSchemaOpt, err := namespace.
		AppendSchemaOptions(nil, protoFile, protoMsg, protoMap, testNamespaceID)
	require.NoError(t, err)
	currentSchemaHist, err := namespace.LoadSchemaHistory(currentSchemaOpt)
	require.NoError(t, err)

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetSchemaHistory(currentSchemaHist))
	require.NoError(t, err)
	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	expectedMeta, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions())
	require.NoError(t, err)
	expectedMap, err := namespace.NewMap([]namespace.Metadata{expectedMeta})
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().CheckAndSet(nsRegKey, 1, gomock.Any()).Return(2, nil).Do(
		func(k string, version int, actualReg *nsproto.Registry) {
			actualMap, err := namespace.FromProto(*actualReg)
			require.NoError(t, err)
			require.NotEmpty(t, actualMap)
			require.True(t, actualMap.Equal(expectedMap))
		})
	err = as.ResetSchema("ns1")
	require.NoError(t, err)
}

func TestAdminService_Crud(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	as := NewAdminService(store, nsRegKey, func() string { return testNamespaceID })
	require.NotNil(t, as)

	expectedOpt := namespace.NewOptions()

	optProto, err := namespace.OptionsToProto(expectedOpt)
	require.NoError(t, err)

	require.NoError(t, as.Add("ns1", optProto))
	require.Error(t, as.Add("ns1", optProto))
	require.NoError(t, as.Set("ns1", optProto))
	require.Error(t, as.Set("ns2", optProto))
	require.NoError(t, as.Add("ns3", optProto))

	nsOpt, err := as.Get("ns1")
	require.NoError(t, err)
	require.NotNil(t, nsOpt)
	nsMeta, err := namespace.ToMetadata("ns1", nsOpt)
	require.NoError(t, err)
	require.True(t, nsMeta.Options().Equal(expectedOpt))

	_, err = as.Get("ns2")
	require.Error(t, err)

	nsReg, err := as.GetAll()
	require.NoError(t, err)
	require.Len(t, nsReg.Namespaces, 2)

	err = as.Delete("ns1")
	require.NoError(t, err)

	nsReg, err = as.GetAll()
	require.NoError(t, err)
	require.Len(t, nsReg.Namespaces, 1)
}

func TestAdminService_DeleteOneNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	currentMeta1, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)
	currentMeta2, err := namespace.NewMetadata(ident.StringID("ns2"), namespace.NewOptions())
	require.NoError(t, err)

	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta1, currentMeta2})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	expectedMeta, err := namespace.NewMetadata(ident.StringID("ns2"), namespace.NewOptions())
	require.NoError(t, err)
	expectedMap, err := namespace.NewMap([]namespace.Metadata{expectedMeta})
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().CheckAndSet(nsRegKey, 1, gomock.Any()).Return(2, nil).Do(
		func(k string, version int, actualReg *nsproto.Registry) {
			actualMap, err := namespace.FromProto(*actualReg)
			require.NoError(t, err)
			require.True(t, actualMap.Equal(expectedMap))
		},
	)

	err = as.Delete("ns1")
	require.NoError(t, err)
}

func TestAdminService_DeleteOneNamespaceFailedSetting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	currentMeta1, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)
	currentMeta2, err := namespace.NewMetadata(ident.StringID("ns2"), namespace.NewOptions())
	require.NoError(t, err)

	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta1, currentMeta2})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().CheckAndSet(nsRegKey, 1, gomock.Any()).Return(-1, errors.New("some error"))

	err = as.Delete("ns1")
	require.Error(t, err)
}

func TestAdminService_DeleteLastNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)

	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().Delete(nsRegKey).Return(nil, nil)

	err = as.Delete("ns1")
	require.NoError(t, err)
}

func TestAdminService_DeleteLastNamespaceFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)

	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)
	storeMock.EXPECT().Delete(nsRegKey).Return(nil, errors.New("some error"))

	err = as.Delete("ns1")
	require.Error(t, err)
}

func TestAdminService_DeleteMissingNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)

	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg, err := namespace.ToProto(currentMap)
	require.NoError(t, err)

	mValue := kv.NewMockValue(ctrl)
	mValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).Do(func(reg *nsproto.Registry) {
		*reg = *currentReg
	})
	mValue.EXPECT().Version().Return(1)
	storeMock.EXPECT().Get(nsRegKey).Return(mValue, nil)

	err = as.Delete("missing-namespace")
	require.EqualError(t, ErrNamespaceNotFound, err.Error())
}

func TestAdminService_DeleteNilRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	as := NewAdminService(storeMock, nsRegKey, func() string { return testNamespaceID })

	storeMock.EXPECT().Get(nsRegKey).Return(nil, errors.New("some error"))

	err := as.Delete("missing-namespace")
	require.Error(t, err)
}
