// Copyright (c) 2024 Uber Technologies, Inc.
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

package server

import (
	ctx "context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/clock"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

func TestStartDebugServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := debug.NewMockZipWriter(ctrl)
	mockWriter.EXPECT().RegisterHandler("/debug/dump", gomock.Any()).Return(nil).AnyTimes()

	logger := zap.NewNop()
	mux := http.NewServeMux()
	debugAddr := "localhost:0"

	stop := startDebugServer(mockWriter, logger, debugAddr, mux)
	defer stop()

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	req, err := http.NewRequestWithContext(ctx.Background(), "GET", testServer.URL+"/debug/pprof/goroutine", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestKvWatchNewSeriesLimitPerShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockTopo := topology.NewMockTopology(ctrl)
	mockTopoMap := topology.NewMockMap(ctrl)
	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	mockShardSet := sharding.NewMockShardSet(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})
	logger := zap.NewNop()
	defaultLimit := 100

	mockStore.EXPECT().Get("m3db.node.cluster-new-series-insert-limit").Return(nil, kv.ErrNotFound)
	mockTopo.EXPECT().Get().Return(mockTopoMap)
	mockTopoMap.EXPECT().ShardSet().Return(mockShardSet)
	mockTopoMap.EXPECT().Replicas().Return(3)
	mockShardSet.EXPECT().AllIDs().Return([]uint32{0, 1, 2, 3})
	mockStore.EXPECT().Watch("m3db.node.cluster-new-series-insert-limit").Return(mockWatch, nil)
	mockRuntimeOptsMgr.EXPECT().Get().Return(runtime.NewOptions())
	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(nil)
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()
	kvWatchNewSeriesLimitPerShard(mockStore, logger, mockTopo, mockRuntimeOptsMgr, defaultLimit)
}

func TestKvWatchEncodersPerBlockLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	logger := zap.NewNop()
	defaultLimit := 10
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})

	mockStore.EXPECT().Get("m3db.node.encoders-per-block-limit").Return(nil, kv.ErrNotFound).AnyTimes()
	mockStore.EXPECT().Watch("m3db.node.encoders-per-block-limit").Return(mockWatch, nil)
	mockRuntimeOptsMgr.EXPECT().Get().Return(runtime.NewOptions()).AnyTimes()
	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(nil).AnyTimes()
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()
	kvWatchEncodersPerBlockLimit(mockStore, logger, mockRuntimeOptsMgr, defaultLimit)

	mockStore.EXPECT().Watch("m3db.node.encoders-per-block-limit").Return(nil, errors.New("watch error"))
	kvWatchEncodersPerBlockLimit(mockStore, logger, mockRuntimeOptsMgr, defaultLimit)
}

func TestKvWatchQueryLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockDocsLimit := limits.NewMockLookbackLimit(ctrl)
	mockBytesReadLimit := limits.NewMockLookbackLimit(ctrl)
	mockDiskSeriesReadLimit := limits.NewMockLookbackLimit(ctrl)
	mockAggregateDocsLimit := limits.NewMockLookbackLimit(ctrl)
	mockDefaultOpts := limits.NewMockOptions(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})
	logger := zap.NewNop()

	mockStore.EXPECT().Get("m3db.query.limits").Return(nil, kv.ErrNotFound).AnyTimes()
	mockStore.EXPECT().Watch("m3db.query.limits").Return(mockWatch, nil)
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()
	kvWatchQueryLimit(mockStore, logger, mockDocsLimit, mockBytesReadLimit,
		mockDiskSeriesReadLimit, mockAggregateDocsLimit, mockDefaultOpts)

	mockStore.EXPECT().Watch("m3db.query.limits").Return(nil, errors.New("watch error"))
	kvWatchQueryLimit(mockStore, logger, mockDocsLimit, mockBytesReadLimit,
		mockDiskSeriesReadLimit, mockAggregateDocsLimit, mockDefaultOpts)
}

func TestKvWatchClientConsistencyLevels(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockClientOpts := client.NewMockAdminOptions(ctrl)
	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})
	logger := zap.NewNop()

	mockStore.EXPECT().Get(gomock.Any()).Return(nil, kv.ErrNotFound).AnyTimes()
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()
	mockStore.EXPECT().Watch(gomock.Any()).Return(mockWatch, nil).Times(5)
	kvWatchClientConsistencyLevels(mockStore, logger, mockClientOpts, mockRuntimeOptsMgr)

	mockStore.EXPECT().Watch(gomock.Any()).Return(nil, errors.New("watch error"))
	kvWatchClientConsistencyLevels(mockStore, logger, mockClientOpts, mockRuntimeOptsMgr)
}

func TestKvWatchStringValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})
	logger := zap.NewNop()
	key := "test_key"

	mockStore.EXPECT().Get(key).Return(nil, kv.ErrNotFound).AnyTimes()
	mockStore.EXPECT().Watch(key).Return(mockWatch, nil)
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()
	kvWatchStringValue(mockStore, logger, key,
		func(v string) error { return nil },
		func() error { return nil })

	mockStore.EXPECT().Watch(key).Return(nil, errors.New("watch error"))
	kvWatchStringValue(mockStore, logger, key,
		func(v string) error { return nil },
		func() error { return nil })
}

func TestSetNewSeriesLimitPerShardOnChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTopo := topology.NewMockTopology(ctrl)
	mockTopoMap := topology.NewMockMap(ctrl)
	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	mockShardSet := sharding.NewMockShardSet(ctrl)
	clusterLimit := 100

	mockRuntimeOptsMgr.EXPECT().Get().Return(runtime.NewOptions()).MinTimes(1)
	mockTopo.EXPECT().Get().Return(mockTopoMap).AnyTimes()
	mockTopoMap.EXPECT().ShardSet().Return(mockShardSet).AnyTimes()
	mockTopoMap.EXPECT().Replicas().Return(3).MinTimes(1)
	mockShardSet.EXPECT().AllIDs().Return([]uint32{0, 1, 2, 3}).MinTimes(1)
	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(nil)
	err := setNewSeriesLimitPerShardOnChange(mockTopo, mockRuntimeOptsMgr, clusterLimit)
	require.NoError(t, err)

	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(errors.New("update error"))
	err = setNewSeriesLimitPerShardOnChange(mockTopo, mockRuntimeOptsMgr, clusterLimit)
	require.Error(t, err)
}

func TestSetEncodersPerBlockLimitOnChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	encoderLimit := 10

	mockRuntimeOptsMgr.EXPECT().Get().Return(runtime.NewOptions()).MinTimes(1)
	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(nil)
	err := setEncodersPerBlockLimitOnChange(mockRuntimeOptsMgr, encoderLimit)
	require.NoError(t, err)

	mockRuntimeOptsMgr.EXPECT().Update(gomock.Any()).Return(errors.New("update error"))
	err = setEncodersPerBlockLimitOnChange(mockRuntimeOptsMgr, encoderLimit)
	require.Error(t, err)
}

func TestWithEncodingAndPoolingOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := config.DBConfiguration{}
	logger := zap.NewNop()
	storageOpts := storage.NewOptions()
	policy := config.PoolingPolicy{}
	err := policy.InitDefaultsAndValidate()
	assert.NoError(t, err)
	opts := withEncodingAndPoolingOptions(cfg, logger, storageOpts, policy)
	assert.NotNil(t, opts)
}

func TestNewAdminClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := client.Configuration{}
	clockOpts := clock.NewOptions()
	iOpts := instrument.NewOptions()
	tchannelOpts := &tchannel.ChannelOptions{}
	mockTopologyInitializer := topology.NewMockInitializer(ctrl)
	mockRuntimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	mockOrigin := topology.NewMockHost(ctrl)
	mockSchemaRegistry := namespace.NewMockSchemaRegistry(ctrl)
	mockKVStore := kv.NewMockStore(ctrl)
	mockContextPool := xcontext.NewPool(xcontext.NewOptions())
	mockCheckedBytesPool := pool.NewMockCheckedBytesPool(ctrl)
	mockIdentifierPool := ident.NewMockPool(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	notifyChannel := make(chan struct{})
	logger := zap.NewNop()

	mockKVStore.EXPECT().Get(gomock.Any()).Return(nil, kv.ErrNotFound).MinTimes(1)
	mockKVStore.EXPECT().Watch(gomock.Any()).Return(mockWatch, nil).MinTimes(1)
	mockWatch.EXPECT().C().Return(notifyChannel).AnyTimes()

	// Test successful client creation
	client, err := newAdminClient(
		config,
		clockOpts,
		iOpts,
		tchannelOpts,
		mockTopologyInitializer,
		mockRuntimeOptsMgr,
		mockOrigin,
		false,
		mockSchemaRegistry,
		mockKVStore,
		mockContextPool,
		mockCheckedBytesPool,
		mockIdentifierPool,
		logger,
		nil,
	)
	require.NoError(t, err)
	assert.NotNil(t, client)
}
