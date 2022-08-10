//// +build integration
//
//// Copyright (c) 2016 Uber Technologies, Inc.
////
//// Permission is hereby granted, free of charge, to any person obtaining a copy
//// of this software and associated documentation files (the "Software"), to deal
//// in the Software without restriction, including without limitation the rights
//// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//// copies of the Software, and to permit persons to whom the Software is
//// furnished to do so, subject to the following conditions:
////
//// The above copyright notice and this permission notice shall be included in
//// all copies or substantial portions of the Software.
////
//// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//// THE SOFTWARE.
//
package integration

//
//import (
//	"testing"
//	"time"
//
//	"github.com/m3db/m3/src/cluster/integration/etcd"
//	"github.com/m3db/m3/src/dbnode/integration/generate"
//	"github.com/m3db/m3/src/dbnode/namespace"
//	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
//	"github.com/m3db/m3/src/x/instrument"
//	xtime "github.com/m3db/m3/src/x/time"
//
//	"github.com/golang/protobuf/proto"
//	"github.com/stretchr/testify/require"
//	"github.com/uber-go/tally"
//)
//
//func TestDynamicNamespaceDelete(t *testing.T) {
//	if testing.Short() {
//		t.SkipNow() // Just skip if we're doing a short run
//	}
//
//	// test options
//	testOpts := NewTestOptions(t).
//		SetTickMinimumInterval(time.Second)
//	require.True(t, len(testOpts.Namespaces()) >= 2)
//	ns0 := testOpts.Namespaces()[0]
//	ns1 := testOpts.Namespaces()[1]
//
//	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
//	scope, closer := tally.NewRootScope(
//		tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
//	defer closer.Close()
//
//	// embedded kv
//	embeddedKV, err := etcd.New(etcd.NewOptions())
//	require.NoError(t, err)
//	defer func() {
//		require.NoError(t, embeddedKV.Close())
//	}()
//	require.NoError(t, embeddedKV.Start())
//	csClient, err := embeddedKV.ConfigServiceClient()
//	require.NoError(t, err)
//	kvStore, err := csClient.KV()
//	require.NoError(t, err)
//
//	// namespace maps
//	protoKey := func(nses ...namespace.Metadata) proto.Message {
//		nsMap, err := namespace.NewMap(nses)
//		require.NoError(t, err)
//
//		registry, err := namespace.ToProto(nsMap)
//		require.NoError(t, err)
//
//		return registry
//	}
//
//	// dynamic namespace registry options
//	dynamicOpts := namespace.NewDynamicOptions().
//		SetConfigServiceClient(csClient).
//		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope))
//	dynamicInit := namespace.NewDynamicInitializer(dynamicOpts)
//	testOpts = testOpts.SetNamespaceInitializer(dynamicInit)
//
//	// initialize value in kv
//	_, err = kvStore.Set(dynamicOpts.NamespaceRegistryKey(), protoKey(ns1))
//	require.NoError(t, err)
//
//	// Test setup
//	testSetup, err := NewTestSetup(t, testOpts, nil)
//	require.NoError(t, err)
//	defer testSetup.Close()
//
//	// Start the server
//	log := testSetup.StorageOpts().InstrumentOptions().Logger()
//	require.NoError(t, testSetup.StartServer())
//
//	// Stop the server
//	defer func() {
//		require.NoError(t, testSetup.StopServer())
//		log.Info("server is now down")
//	}()
//
//	// Write test data
//	blockSize := ns0.Options().RetentionOptions().BlockSize()
//	now := testSetup.NowFn()()
//	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
//	inputData := []generate.BlockConfig{
//		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
//		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(blockSize)},
//	}
//	for _, input := range inputData {
//		start := input.Start
//		testData := generate.Block(input)
//		seriesMaps[start] = testData
//	}
//	log.Info("test data is now generated")
//
//	// fail to write to non-existent namespaces
//	for _, testData := range seriesMaps {
//		require.Error(t, testSetup.WriteBatch(ns0.ID(), testData))
//	}
//
//	// delete namespace key, ensure update propagates
//	numInvalid := numInvalidNamespaceUpdates(reporter)
//	_, err = kvStore.Delete(dynamicOpts.NamespaceRegistryKey())
//	require.NoError(t, err)
//	deletePropagated := func() bool {
//		return numInvalidNamespaceUpdates(reporter) > numInvalid
//	}
//	require.True(t, waitUntil(deletePropagated, 20*time.Second))
//	log.Info("deleted namespace key propagated from KV to testSetup")
//
//	// update value in kv
//	_, err = kvStore.Set(dynamicOpts.NamespaceRegistryKey(), protoKey(ns0, ns1))
//	require.NoError(t, err)
//	log.Info("new namespace added to kv")
//
//	// wait until the new namespace is registered
//	nsExists := func() bool {
//		_, ok := testSetup.DB().Namespace(ns0.ID())
//		return ok
//	}
//	require.True(t, waitUntil(nsExists, 5*time.Second))
//	log.Info("new namespace propagated from KV to testSetup")
//
//	// write to new namespace
//	for start, testData := range seriesMaps {
//		testSetup.SetNowFn(start)
//		require.NoError(t, testSetup.WriteBatch(ns0.ID(), testData))
//	}
//	log.Info("test data is now written")
//
//	// Advance time and sleep for a long enough time so data blocks are sealed during ticking
//	testSetup.SetNowFn(testSetup.NowFn()().Add(2 * blockSize))
//	later := testSetup.NowFn()()
//	testSetup.SleepFor10xTickMinimumInterval()
//
//	metadatasByShard := testSetupMetadatas(t, testSetup, ns0.ID(), now, later)
//	observedSeriesMaps := testSetupToSeriesMaps(t, testSetup, ns0, metadatasByShard)
//	log.Info("reading data from testSetup")
//
//	// Verify retrieved data matches what we've written
//	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)
//	log.Info("data is verified")
//}
//
//func numInvalidNamespaceUpdates(reporter xmetrics.TestStatsReporter) int64 {
//	return reporter.Counters()["namespace-registry.invalid-update"]
//}
