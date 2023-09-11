// Copyright (c) 2020  Uber Technologies, Inc.
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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	defaultTestNs1ID         = ident.StringID("testns1")
	defaultTestNs2ID         = ident.StringID("testns2")
	defaultTestRetentionOpts = retention.NewOptions().
					SetBufferFuture(10 * time.Minute).
					SetBufferPast(10 * time.Minute).
					SetBlockSize(2 * time.Hour).
					SetRetentionPeriod(48 * time.Hour)
	defaultTestNs2RetentionOpts = defaultTestRetentionOpts.
					SetBlockSize(4 * time.Hour)
	defaultTestAggregationOpts = namespace.NewAggregationOptions().
					SetAggregations([]namespace.Aggregation{namespace.NewUnaggregatedAggregation()})
	defaultTestNs2AggregationOpts = namespace.NewAggregationOptions().
					SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
			namespace.AggregatedAttributes{
				Resolution:        1 * time.Minute,
				DownsampleOptions: namespace.NewDownsampleOptions(true),
			}),
		})
	defaultTestNs1Opts = newNamespaceOptions().SetRetentionOptions(defaultTestRetentionOpts).
				SetAggregationOptions(defaultTestAggregationOpts)
	defaultTestNs2Opts = newNamespaceOptions().SetRetentionOptions(defaultTestNs2RetentionOpts).
				SetAggregationOptions(defaultTestNs2AggregationOpts)
)

func newNamespaceOptions() namespace.Options {
	state, err := namespace.NewStagingState(nsproto.StagingStatus_READY)
	if err != nil {
		panic("error creating staging state")
	}
	return namespace.NewOptions().SetStagingState(state)
}

func TestDynamicClustersUninitialized(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	// setup dynamic cluster without any namespaces
	mapCh := make(nsMapCh, 10)
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, false)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := newTestOptions(cfg)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	//nolint:errcheck
	defer clusters.Close()

	// Aggregated namespaces should not exist
	_, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  48 * time.Hour,
		Resolution: 1 * time.Minute,
	})
	require.False(t, ok)

	// Unaggregated namespaces should not be initialized
	_, ok = clusters.UnaggregatedClusterNamespace()
	require.False(t, ok)

	// Cluster namespaces should be empty
	require.Len(t, clusters.ClusterNamespaces(), 0)
}

func TestDynamicClustersInitialization(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts},
	})
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, true)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := newTestOptions(cfg)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer clusters.Close()

	requireClusterNamespace(t, clusters, defaultTestNs2ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  1 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	})

	requireClusterNamespace(t, clusters, defaultTestNs1ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}})

	requireClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs1ID, defaultTestNs2ID})
}

func TestDynamicClustersWithUpdates(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	nsMap := testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts},
	})
	mapCh <- nsMap
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, true)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := newTestOptions(cfg)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer clusters.Close()

	requireClusterNamespace(t, clusters, defaultTestNs2ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  1 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	})

	requireClusterNamespace(t, clusters, defaultTestNs1ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}})

	// Update resolution of aggregated namespace
	newOpts := defaultTestNs2Opts.
		SetAggregationOptions(defaultTestNs2AggregationOpts.
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        2 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(true),
				}),
			}))
	nsMap = testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: newOpts},
	})

	// Send update to trigger watch
	mapCh <- nsMap

	require.True(t, xclock.WaitUntil(func() bool {
		found := assertClusterNamespace(clusters, defaultTestNs2ID, ClusterNamespaceOptions{
			attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   48 * time.Hour,
				Resolution:  2 * time.Minute,
			},
			downsample: &ClusterNamespaceDownsampleOptions{All: true},
		})

		return found && assertClusterNamespaceIDs(clusters.ClusterNamespaces(), []ident.ID{defaultTestNs1ID, defaultTestNs2ID})
	}, time.Second))
}

func TestDynamicClustersWithMultipleInitializers(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)
	mockSession2 := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts},
	})
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, true)

	fooOpts := defaultTestNs1Opts.
		SetAggregationOptions(namespace.NewAggregationOptions().
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        2 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(true),
				}),
			}))
	barOpts := defaultTestNs1Opts.
		SetAggregationOptions(namespace.NewAggregationOptions().
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        5 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(false),
				}),
			}))
	var (
		fooNsID = ident.StringID("foo")
		barNsID = ident.StringID("bar")
	)
	nsMap := testNamespaceMap(t, []mapParams{
		{nsID: fooNsID, nsOpts: fooOpts},
		{nsID: barNsID, nsOpts: barOpts},
	})

	mapCh2 := make(nsMapCh, 10)
	mapCh2 <- nsMap
	nsInitializer2 := newFakeNsInitializer(t, ctrl, mapCh2, true)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}
	cfg2 := DynamicClusterNamespaceConfiguration{
		session:       mockSession2,
		nsInitializer: nsInitializer2,
	}

	opts := newTestOptions(cfg, cfg2)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer clusters.Close()

	requireClusterNamespace(t, clusters, defaultTestNs2ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  1 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	})

	requireClusterNamespace(t, clusters, fooNsID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  2 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	})

	requireClusterNamespace(t, clusters, barNsID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  5 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: false},
	})

	requireClusterNamespace(t, clusters, defaultTestNs1ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}})

	requireClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs1ID, defaultTestNs2ID,
		fooNsID, barNsID})
}

func TestDynamicClustersNonReadyNamespace(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	state, err := namespace.NewStagingState(nsproto.StagingStatus_INITIALIZING)
	require.NoError(t, err)

	state2, err := namespace.NewStagingState(nsproto.StagingStatus_UNKNOWN)
	require.NoError(t, err)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts.SetStagingState(state)},
		{nsID: ident.StringID("foo"), nsOpts: defaultTestNs2Opts.SetStagingState(state2)},
	})
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, true)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := newTestOptions(cfg)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer clusters.Close()

	requireClusterNamespace(t, clusters, defaultTestNs1ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}})

	requireClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs1ID})
	requireNonReadyClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs2ID, ident.StringID("foo")})
}

func TestDynamicClustersEmptyNamespacesThenUpdates(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh, false)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := newTestOptions(cfg)

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer clusters.Close()

	requireClusterNamespaceIDs(t, clusters, []ident.ID{})

	// Send update to trigger watch and add namespaces.
	nsMap := testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts},
	})
	mapCh <- nsMap

	require.True(t, xclock.WaitUntil(func() bool {
		found := assertClusterNamespace(clusters, defaultTestNs2ID, ClusterNamespaceOptions{
			attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   48 * time.Hour,
				Resolution:  1 * time.Minute,
			},
			downsample: &ClusterNamespaceDownsampleOptions{All: true},
		})

		found = found && assertClusterNamespace(clusters, defaultTestNs1ID, ClusterNamespaceOptions{
			attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.UnaggregatedMetricsType,
				Retention:   48 * time.Hour,
			}})

		return found && assertClusterNamespaceIDs(clusters.ClusterNamespaces(), []ident.ID{defaultTestNs1ID, defaultTestNs2ID})
	}, time.Second))
}

func TestDynamicClustersInitFailures(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nil, errors.New("failed to init")).AnyTimes()

	cfg := DynamicClusterNamespaceConfiguration{
		session: mockSession,
		nsInitializer: &fakeNsInitializer{
			registry: reg,
		},
	}

	opts := newTestOptions(cfg)

	_, err := NewDynamicClusters(opts)
	require.Error(t, err)
}

func newTestOptions(cfgs ...DynamicClusterNamespaceConfiguration) DynamicClusterOptions {
	return NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration(cfgs).
		SetInstrumentOptions(instrument.NewOptions()).
		SetClusterNamespacesWatcher(NewClusterNamespacesWatcher())
}

func requireClusterNamespace(t *testing.T, clusters Clusters, expectedID ident.ID, expectedOpts ClusterNamespaceOptions) {
	require.True(t, assertClusterNamespace(clusters, expectedID, expectedOpts))
}

func assertClusterNamespace(clusters Clusters, expectedID ident.ID, expectedOpts ClusterNamespaceOptions) bool {
	var (
		ns ClusterNamespace
		ok bool
	)
	if expectedOpts.Attributes().MetricsType == storagemetadata.AggregatedMetricsType {
		if ns, ok = clusters.AggregatedClusterNamespace(RetentionResolution{
			Retention:  expectedOpts.Attributes().Retention,
			Resolution: expectedOpts.Attributes().Resolution,
		}); !ok {
			return false
		}
	} else {
		ns, ok = clusters.UnaggregatedClusterNamespace()
		if !ok {
			return false
		}
	}
	return assert.ObjectsAreEqual(expectedID.String(), ns.NamespaceID().String()) &&
		assert.ObjectsAreEqual(expectedOpts, ns.Options())
}

type mapParams struct {
	nsID   ident.ID
	nsOpts namespace.Options
}

func requireNonReadyClusterNamespaceIDs(t *testing.T, clusters Clusters, ids []ident.ID) {
	require.True(t, assertClusterNamespaceIDs(clusters.NonReadyClusterNamespaces(), ids))
}

func requireClusterNamespaceIDs(t *testing.T, clusters Clusters, ids []ident.ID) {
	require.True(t, assertClusterNamespaceIDs(clusters.ClusterNamespaces(), ids))
}

func assertClusterNamespaceIDs(actual ClusterNamespaces, ids []ident.ID) bool {
	var (
		nsIds       = make([]string, 0, len(ids))
		expectedIds = make([]string, 0, len(ids))
	)
	for _, ns := range actual {
		nsIds = append(nsIds, ns.NamespaceID().String())
	}
	for _, id := range ids {
		expectedIds = append(expectedIds, id.String())
	}
	sort.Strings(nsIds)
	sort.Strings(expectedIds)
	return assert.ObjectsAreEqual(expectedIds, nsIds)
}

func testNamespaceMap(t *testing.T, params []mapParams) namespace.Map {
	var mds []namespace.Metadata
	for _, param := range params {
		md, err := namespace.NewMetadata(param.nsID, param.nsOpts)
		require.NoError(t, err)
		mds = append(mds, md)
	}
	nsMap, err := namespace.NewMap(mds)
	require.NoError(t, err)
	return nsMap
}

type nsMapCh chan namespace.Map

type fakeNsInitializer struct {
	registry *namespace.MockRegistry
}

func (m *fakeNsInitializer) Init() (namespace.Registry, error) {
	return m.registry, nil
}

func newFakeNsInitializer(
	t *testing.T,
	ctrl *gomock.Controller,
	nsMapCh nsMapCh,
	withInitialValue bool,
) *fakeNsInitializer {
	watch := xwatch.NewWatchable()

	if withInitialValue {
		initialValue := <-nsMapCh
		err := watch.Update(initialValue)
		require.NoError(t, err)
	}

	go func() {
		for {
			v, ok := <-nsMapCh
			if !ok { // closed channel
				return
			}

			err := watch.Update(v)
			require.NoError(t, err)
		}
	}()

	_, w, err := watch.Watch()
	require.NoError(t, err)

	nsWatch := namespace.NewWatch(w)
	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nsWatch, nil).AnyTimes()

	return &fakeNsInitializer{
		registry: reg,
	}
}
