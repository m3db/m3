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
	"testing"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	logging.InitWithCores(nil)

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
			"metrics-ns1": &nsproto.NamespaceOptions{
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
			"metrics-ns2": &nsproto.NamespaceOptions{
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
