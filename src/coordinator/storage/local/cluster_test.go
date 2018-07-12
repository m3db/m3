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

package local

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClustersWithDuplicateAggregatedClusterNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_unagg"),
		Session:     client.NewMockSession(ctrl),
		Retention:   2 * 24 * time.Hour,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_agg0"),
		Session:     client.NewMockSession(ctrl),
		Retention:   7 * 24 * time.Hour,
		Resolution:  time.Minute,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("metrics_agg1"),
		Session:     client.NewMockSession(ctrl),
		Retention:   7 * 24 * time.Hour,
		Resolution:  time.Minute,
	})
	require.Error(t, err)

	str := err.Error()
	assert.True(t, strings.Contains(str, "duplicate aggregated namespace"),
		fmt.Sprintf("unexpected error: %s", err.Error()))
}

func TestNewClustersFromConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	newClient1, mockSession1 := newTestClientFromConfig(ctrl)
	newClient2, mockSession2 := newTestClientFromConfig(ctrl)
	cfg := ClustersStaticConfiguration{
		ClusterStaticConfiguration{
			newClientFromConfig: newClient1,
			Namespaces: []ClusterStaticNamespaceConfiguration{
				ClusterStaticNamespaceConfiguration{
					Namespace:          "unaggregated",
					StorageMetricsType: storage.UnaggregatedMetricsType,
					Retention:          7 * 24 * time.Hour,
				},
			},
		},
		ClusterStaticConfiguration{
			newClientFromConfig: newClient2,
			Namespaces: []ClusterStaticNamespaceConfiguration{
				ClusterStaticNamespaceConfiguration{
					Namespace:          "aggregated0",
					StorageMetricsType: storage.AggregatedMetricsType,
					Retention:          30 * 24 * time.Hour,
					Resolution:         time.Minute,
				},
				ClusterStaticNamespaceConfiguration{
					Namespace:          "aggregated1",
					StorageMetricsType: storage.AggregatedMetricsType,
					Retention:          365 * 24 * time.Hour,
					Resolution:         10 * time.Minute,
				},
			},
		},
	}

	clusters, err := cfg.NewClusters(ClustersStaticConfigurationOptions{})
	require.NoError(t, err)

	// Resolve expected clusters and check attributes
	unaggregatedNs := clusters.UnaggregatedClusterNamespace()
	assert.Equal(t, "unaggregated", unaggregatedNs.NamespaceID().String())
	assert.Equal(t, storage.Attributes{
		MetricsType: storage.UnaggregatedMetricsType,
		Retention:   7 * 24 * time.Hour,
	}, unaggregatedNs.Attributes())
	assert.True(t, mockSession1 == unaggregatedNs.Session())

	aggregated1Month1Minute, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  30 * 24 * time.Hour,
		Resolution: time.Minute,
	})
	require.True(t, ok)
	assert.Equal(t, "aggregated0", aggregated1Month1Minute.NamespaceID().String())
	assert.Equal(t, storage.Attributes{
		MetricsType: storage.AggregatedMetricsType,
		Retention:   30 * 24 * time.Hour,
		Resolution:  time.Minute,
	}, aggregated1Month1Minute.Attributes())
	assert.True(t, mockSession2 == aggregated1Month1Minute.Session())

	aggregated1Year10Minute, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  365 * 24 * time.Hour,
		Resolution: 10 * time.Minute,
	})
	require.True(t, ok)
	assert.Equal(t, "aggregated1", aggregated1Year10Minute.NamespaceID().String())
	assert.Equal(t, storage.Attributes{
		MetricsType: storage.AggregatedMetricsType,
		Retention:   365 * 24 * time.Hour,
		Resolution:  10 * time.Minute,
	}, aggregated1Year10Minute.Attributes())
	assert.True(t, mockSession2 == aggregated1Year10Minute.Session())

	// Ensure cannot resolve unexpected clusters
	_, ok = clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  time.Hour,
		Resolution: time.Minute,
	})
	require.False(t, ok)

	// Close sessions at most once each
	mockSession1.EXPECT().Close().Return(nil).Times(1)
	mockSession2.EXPECT().Close().Return(nil).Times(1)

	err = clusters.Close()
	require.NoError(t, err)
}

func newTestClientFromConfig(ctrl *gomock.Controller) (
	newClientFromConfig,
	*client.MockSession,
) {
	mockSession := client.NewMockSession(ctrl)

	mockClient := client.NewMockClient(ctrl)
	mockClient.EXPECT().DefaultSession().Return(mockSession, nil).AnyTimes()

	newClientFn := func(
		_ client.Configuration,
		_ client.ConfigurationParameters,
		_ ...client.CustomOption,
	) (client.Client, error) {
		return mockClient, nil
	}

	return newClientFn, mockSession
}
