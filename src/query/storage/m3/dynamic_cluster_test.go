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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/golang/mock/gomock"
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

func TestDynamicClustersInitialization(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t, []mapParams{
		{nsID: defaultTestNs1ID, nsOpts: defaultTestNs1Opts},
		{nsID: defaultTestNs2ID, nsOpts: defaultTestNs2Opts},
	})
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration([]DynamicClusterNamespaceConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer func() {
		<-nsInitializer.updateCh
		clusters.Close()
	}()

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
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration([]DynamicClusterNamespaceConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer func() {
		<-nsInitializer.updateCh
		<-nsInitializer.updateCh
		clusters.Close()
	}()

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

	for {
		select {
		case <-time.After(1 * time.Second):
			require.Fail(t, "failed to receive namespace watch update")
		default:
			if _, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
				Retention:  48 * time.Hour,
				Resolution: 2 * time.Minute,
			}); ok {
				requireClusterNamespace(t, clusters, defaultTestNs2ID, ClusterNamespaceOptions{
					attributes: storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Retention:   48 * time.Hour,
						Resolution:  2 * time.Minute,
					},
					downsample: &ClusterNamespaceDownsampleOptions{All: true},
				})

				requireClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs1ID, defaultTestNs2ID})

				return
			}
		}
	}
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
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh)

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
	nsInitializer2 := newFakeNsInitializer(t, ctrl, mapCh2)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}
	cfg2 := DynamicClusterNamespaceConfiguration{
		session:       mockSession2,
		nsInitializer: nsInitializer2,
	}

	opts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration([]DynamicClusterNamespaceConfiguration{cfg, cfg2}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer func() {
		<-nsInitializer.updateCh
		<-nsInitializer2.updateCh
		clusters.Close()
	}()

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

func TestDynamicClustersNotReadyNamespace(t *testing.T) {
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
	nsInitializer := newFakeNsInitializer(t, ctrl, mapCh)

	cfg := DynamicClusterNamespaceConfiguration{
		session:       mockSession,
		nsInitializer: nsInitializer,
	}

	opts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration([]DynamicClusterNamespaceConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	defer func() {
		<-nsInitializer.updateCh
		clusters.Close()
	}()

	requireClusterNamespace(t, clusters, defaultTestNs1ID, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}})

	requireClusterNamespaceIDs(t, clusters, []ident.ID{defaultTestNs1ID})
}

func TestDynamicClustersInitFailures(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	updateCh := make(chan struct{}, 10)
	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nil, errors.New("failed to init")).AnyTimes()

	cfg := DynamicClusterNamespaceConfiguration{
		session: mockSession,
		nsInitializer: &fakeNsInitializer{
			registry: reg,
			updateCh: updateCh,
		},
	}

	opts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration([]DynamicClusterNamespaceConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	_, err := NewDynamicClusters(opts)
	require.Error(t, err)
}

func requireClusterNamespace(t *testing.T, clusters Clusters, expectedID ident.ID, expectedOpts ClusterNamespaceOptions) {
	var (
		ns ClusterNamespace
		ok bool
	)
	if expectedOpts.Attributes().MetricsType == storagemetadata.AggregatedMetricsType {
		ns, ok = clusters.AggregatedClusterNamespace(RetentionResolution{
			Retention:  expectedOpts.Attributes().Retention,
			Resolution: expectedOpts.Attributes().Resolution,
		})
		if !ok {
			require.Fail(t, fmt.Sprintf("aggregated namespace not found for resolution=%v retention=%v",
				expectedOpts.Attributes().Resolution, expectedOpts.Attributes().Retention))
		}
	} else {
		ns = clusters.UnaggregatedClusterNamespace()
	}
	require.Equal(t, expectedID.String(), ns.NamespaceID().String())
	require.Equal(t, expectedOpts, ns.Options())
}

type mapParams struct {
	nsID   ident.ID
	nsOpts namespace.Options
}

func requireClusterNamespaceIDs(t *testing.T, clusters Clusters, ids []ident.ID) {
	var (
		nsIds       = make([]string, 0, len(ids))
		expectedIds = make([]string, 0, len(ids))
	)
	for _, ns := range clusters.ClusterNamespaces() {
		nsIds = append(nsIds, ns.NamespaceID().String())
	}
	for _, id := range ids {
		expectedIds = append(expectedIds, id.String())
	}
	sort.Strings(nsIds)
	sort.Strings(expectedIds)
	require.Equal(t, expectedIds, nsIds)
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
	updateCh chan struct{}
}

func (m *fakeNsInitializer) Init() (namespace.Registry, error) {
	return m.registry, nil
}

func newFakeNsInitializer(
	t *testing.T,
	ctrl *gomock.Controller,
	nsMapCh nsMapCh,
) *fakeNsInitializer {
	updateCh := make(chan struct{}, 10)
	watch := xwatch.NewWatchable()
	go func() {
		for {
			v, ok := <-nsMapCh
			if !ok { // closed channel
				return
			}

			watch.Update(v)
			updateCh <- struct{}{}
		}
	}()

	_, w, err := watch.Watch()
	require.NoError(t, err)

	nsWatch := namespace.NewWatch(w)
	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nsWatch, nil).AnyTimes()

	return &fakeNsInitializer{
		registry: reg,
		updateCh: updateCh,
	}
}
