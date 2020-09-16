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
	"errors"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	xjson "github.com/m3db/m3/src/x/json"

	"github.com/gogo/protobuf/proto"
	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type storeOptionsMatcher struct {
	zone        string
	namespace   string
	environment string
}

func (s storeOptionsMatcher) Matches(x interface{}) bool {
	opts := x.(kv.OverrideOptions)
	if s.zone != "" && s.zone != opts.Zone() {
		return false
	}
	if s.namespace != "" && s.namespace != opts.Namespace() {
		return false
	}
	if s.environment != "" && s.environment != opts.Environment() {
		return false
	}
	return true
}

func (s storeOptionsMatcher) String() string {
	return fmt.Sprintf("checks that zone=%s, namespace=%s, environment=%s", s.zone, s.namespace, s.environment)
}

func newStoreOptionsMatcher(zone, namespace, environment string) gomock.Matcher {
	return storeOptionsMatcher{
		zone:        zone,
		namespace:   namespace,
		environment: environment,
	}
}

func TestMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)

	// Test KV get error
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, errors.New("unable to get key"))
	meta, version, err := Metadata(mockKV)
	assert.Nil(t, meta)
	assert.Equal(t, -1, version)
	assert.EqualError(t, err, "unable to get key")

	// Test empty namespace
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	meta, version, err = Metadata(mockKV)
	assert.NotNil(t, meta)
	assert.Equal(t, 0, version)
	assert.Len(t, meta, 0)
	assert.NoError(t, err)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"metrics-ns1": {
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: false,
				CleanupEnabled:    false,
				RepairEnabled:     false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     200000000000,
					BlockSizeNanos:                           100000000000,
					BufferFutureNanos:                        3000000000,
					BufferPastNanos:                          4000000000,
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: 5000000000,
				},
			},
			"metrics-ns2": {
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    true,
				RepairEnabled:     false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     400000000000,
					BlockSizeNanos:                           300000000000,
					BufferFutureNanos:                        8000000000,
					BufferPastNanos:                          9000000000,
					BlockDataExpiry:                          false,
					BlockDataExpiryAfterNotAccessPeriodNanos: 10000000000,
				},
			},
		},
	}
	mockMetaValue := kv.NewMockValue(ctrl)
	mockMetaValue.EXPECT().Unmarshal(gomock.Not(nil)).Return(nil).SetArg(0, registry)
	mockMetaValue.EXPECT().Version().Return(0)

	// Test namespaces
	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockMetaValue, nil)
	meta, version, err = Metadata(mockKV)
	assert.NotNil(t, meta)
	assert.Equal(t, 0, version)
	assert.Len(t, meta, 2)
	assert.NoError(t, err)
}


type testExtendedOptions struct {
	value string
}

func (o *testExtendedOptions) Validate() error {
	return nil
}

func (o *testExtendedOptions) ToProto() (proto.Message, string) {
	return &protobuftypes.StringValue{Value: o.value}, testTypeUrlPrefix
}

func convertToTestExtendedOptions(msg proto.Message) (namespace.ExtendedOptions, error) {
	strVal := msg.(*protobuftypes.StringValue)
	return &testExtendedOptions{strVal.Value}, nil
}

func init() {
	namespace.RegisterExtendedOptionsConverter(testTypeUrlPrefix, &protobuftypes.StringValue{}, convertToTestExtendedOptions)
}

func newTestExtendedOptionsProto(s string) *protobuftypes.Any {
	// NB: using the stock StringValue message so that we don't have to introduce any new protobuf just for tests.
	strMsg := &protobuftypes.StringValue{Value: s}
	serializedMsg, _ := proto.Marshal(strMsg)

	return &protobuftypes.Any{
		TypeUrl: testTypeUrlPrefix + proto.MessageName(strMsg),
		Value:   serializedMsg,
	}
}

func testExtendedOptionsJson(s string) xjson.Map {
	return xjson.Map{
		"@type": testTypeUrlPrefix + "google.protobuf.StringValue",
		"value": s,
	}
}
