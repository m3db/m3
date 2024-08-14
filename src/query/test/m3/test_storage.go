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

package m3

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/sync"
)

const (
	// TestNamespaceID is the namespace of the test unaggregated namespace
	// used by local storage.
	TestNamespaceID = "metrics"
	// TestRetention is the retention of the test unaggregated namespace
	// used by local storage.
	TestRetention = 30 * 24 * time.Hour
)

var (
	defaultLookbackDuration = time.Minute
)

// NewStorageAndSession generates a new m3 storage and mock session
func NewStorageAndSession(
	t *testing.T,
	ctrl *gomock.Controller,
) (storage.Storage, *client.MockSession) {
	session := client.NewMockSession(ctrl)
	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID(TestNamespaceID),
		Session:     session,
		Retention:   TestRetention,
	})
	require.NoError(t, err)
	writePool, err := sync.NewPooledWorkerPool(10,
		sync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	writePool.Init()
	tagOptions := models.NewTagOptions().SetMetricName([]byte("name"))
	opts := m3.NewOptions(encoding.NewOptions()).
		SetWriteWorkerPool(writePool).
		SetTagOptions(tagOptions).
		SetLookbackDuration(defaultLookbackDuration)

	storage, err := m3.NewStorage(clusters, opts, instrument.NewOptions())
	require.NoError(t, err)
	return storage, session
}

// NewStorageAndSessionWithAggregatedNamespaces generates a new m3 storage and mock session
// with the specified aggregated cluster namespace definitions.
func NewStorageAndSessionWithAggregatedNamespaces(
	t *testing.T,
	ctrl *gomock.Controller,
	aggregatedNamespaces []m3.AggregatedClusterNamespaceDefinition,
) (storage.Storage, *client.MockSession) {
	session := client.NewMockSession(ctrl)
	for i := range aggregatedNamespaces {
		aggregatedNamespaces[i].Session = session
	}

	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID(TestNamespaceID),
		Session:     session,
		Retention:   TestRetention,
	}, aggregatedNamespaces...)
	require.NoError(t, err)

	writePool, err := sync.NewPooledWorkerPool(10,
		sync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	writePool.Init()
	tagOptions := models.NewTagOptions().SetMetricName([]byte("name"))
	opts := m3.NewOptions(encoding.NewOptions()).
		SetWriteWorkerPool(writePool).
		SetTagOptions(tagOptions).
		SetLookbackDuration(defaultLookbackDuration)

	storage, err := m3.NewStorage(clusters, opts, instrument.NewOptions())
	require.NoError(t, err)
	return storage, session
}
