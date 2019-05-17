package kvadmin

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"

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
)

func TestAdminService_DeploySchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storeMock := kv.NewMockStore(ctrl)
	var nsRegKey = "nsRegKey"
	as := NewAdminService(storeMock, nsRegKey)
	require.NotNil(t, as)

	currentMeta, err := namespace.NewMetadata(ident.StringID("ns1"), namespace.NewOptions())
	require.NoError(t, err)
	currentMap, err := namespace.NewMap([]namespace.Metadata{currentMeta})
	require.NoError(t, err)
	currentReg := namespace.ToProto(currentMap)

	protoFile := "mainpkg/test.proto"
	protoMap := map[string]string{"mainpkg/test.proto": mainProtoStr, "mainpkg/imported.proto": importedProtoStr}
	expectedSchemaOpt, err := namespace.AppendSchemaOptions(nil, protoFile, "mainpkg.TestMessage", protoMap, "first")
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
	require.NoError(t, as.DeploySchema("ns1", protoFile,
		"mainpkg.TestMessage", protoMap, "first"))
}
