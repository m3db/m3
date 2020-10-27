// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newTestSetup(t *testing.T, ctrl *gomock.Controller, watchable kv.ValueWatchable) (DynamicOptions, *kv.MockStore) {
	_, watch, err := watchable.Watch()
	require.NoError(t, err)

	ts := tally.NewTestScope("", nil)
	mockKVStore := kv.NewMockStore(ctrl)
	mockKVStore.EXPECT().Watch(defaultNsRegistryKey).Return(watch, nil)

	mockCSClient := client.NewMockClient(ctrl)
	mockCSClient.EXPECT().KV().Return(mockKVStore, nil)

	opts := NewDynamicOptions().
		SetInstrumentOptions(
			instrument.NewOptions().
				SetReportInterval(10 * time.Millisecond).
				SetMetricsScope(ts)).
		SetConfigServiceClient(mockCSClient)

	return opts, mockKVStore
}

func numInvalidUpdates(opts DynamicOptions) int64 {
	scope := opts.InstrumentOptions().MetricsScope().(tally.TestScope)
	count, ok := scope.Snapshot().Counters()["namespace-registry.invalid-update+"]
	if !ok {
		return 0
	}
	return count.Value()
}

func currentVersionMetrics(opts DynamicOptions) float64 {
	scope := opts.InstrumentOptions().MetricsScope().(tally.TestScope)
	g, ok := scope.Snapshot().Gauges()["namespace-registry.current-version+"]
	if !ok {
		return 0.0
	}
	return g.Value()
}

func TestInitializerNoTimeout(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	value := singleTestValue()
	expectedNsValue := value.Namespaces["testns1"]
	w := newTestWatchable(t, value)
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)
	reg, err := init.Init()
	require.NoError(t, err)

	requireNamespaceInRegistry(t, reg, expectedNsValue)

	require.NoError(t, reg.Close())
}

func TestInitializerUpdateWithBadProto(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := newTestWatchable(t, singleTestValue())
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	reg, err := init.Init()
	require.NoError(t, err)

	rmap, err := reg.Watch()
	require.NoError(t, err)
	require.Len(t, rmap.Get().Metadatas(), 1)
	require.Equal(t, int64(0), numInvalidUpdates(opts))

	// update with bad proto
	require.NoError(t, w.Update(&testValue{
		version: 2,
		Registry: nsproto.Registry{
			Namespaces: map[string]*nsproto.NamespaceOptions{
				"testns1": nil,
				"testns2": nil,
			},
		},
	}))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(1), numInvalidUpdates(opts))

	require.Len(t, rmap.Get().Metadatas(), 1)
	require.NoError(t, reg.Close())
}

func TestInitializerUpdateWithOlderVersion(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initValue := singleTestValue()
	w := newTestWatchable(t, initValue)
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	reg, err := init.Init()
	require.NoError(t, err)

	rmap, err := reg.Watch()
	require.NoError(t, err)
	require.Len(t, rmap.Get().Metadatas(), 1)
	require.Equal(t, int64(0), numInvalidUpdates(opts))

	// update with bad version
	require.NoError(t, w.Update(&testValue{
		version:  1,
		Registry: initValue.Registry,
	}))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(1), numInvalidUpdates(opts))

	require.Len(t, rmap.Get().Metadatas(), 1)
	require.NoError(t, reg.Close())
}

func TestInitializerUpdateWithNilValue(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := newTestWatchable(t, singleTestValue())
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	reg, err := init.Init()
	require.NoError(t, err)

	rmap, err := reg.Watch()
	require.NoError(t, err)
	require.Len(t, rmap.Get().Metadatas(), 1)
	require.Equal(t, int64(0), numInvalidUpdates(opts))

	// update with nil value
	require.NoError(t, w.Update(nil))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(1), numInvalidUpdates(opts))

	require.Len(t, rmap.Get().Metadatas(), 1)
	require.NoError(t, reg.Close())
}

func TestInitializerUpdateWithNilInitialValue(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := newTestWatchable(t, nil)
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	require.NoError(t, w.Update(nil))
	_, err := init.Init()
	require.Error(t, err)
}

func TestInitializerUpdateWithIdenticalValue(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initValue := singleTestValue()
	w := newTestWatchable(t, initValue)
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	reg, err := init.Init()
	require.NoError(t, err)

	rmap, err := reg.Watch()
	require.NoError(t, err)
	require.Len(t, rmap.Get().Metadatas(), 1)
	require.Equal(t, int64(0), numInvalidUpdates(opts))

	// update with new version
	require.NoError(t, w.Update(&testValue{
		version:  2,
		Registry: initValue.Registry,
	}))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(1), numInvalidUpdates(opts))

	require.Len(t, rmap.Get().Metadatas(), 1)
	require.NoError(t, reg.Close())
}

func TestInitializerUpdateSuccess(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initValue := singleTestValue()
	w := newTestWatchable(t, initValue)
	defer w.Close()

	opts, _ := newTestSetup(t, ctrl, w)
	init := NewDynamicInitializer(opts)

	reg, err := init.Init()
	require.NoError(t, err)

	rmap, err := reg.Watch()
	require.NoError(t, err)
	require.Len(t, rmap.Get().Metadatas(), 1)
	require.Equal(t, int64(0), numInvalidUpdates(opts))
	require.Equal(t, 0., currentVersionMetrics(opts))

	// update with valid value
	require.NoError(t, w.Update(&testValue{
		version: 2,
		Registry: nsproto.Registry{
			Namespaces: map[string]*nsproto.NamespaceOptions{
				"testns1": initValue.Namespaces["testns1"],
				"testns2": initValue.Namespaces["testns1"],
			},
		},
	}))

	for {
		time.Sleep(20 * time.Millisecond)
		if numInvalidUpdates(opts) != 0 {
			continue
		}
		if currentVersionMetrics(opts) != 2. {
			continue
		}
		if len(rmap.Get().Metadatas()) != 2 {
			continue
		}
		break
	}
	require.NoError(t, reg.Close())
}

func TestInitializerAllowEmptyEnabled_EmptyRegistry(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := newTestWatchable(t, nil)
	defer w.Close()

	opts, mockKV := newTestSetup(t, ctrl, w)
	opts = opts.SetAllowEmptyInitialNamespaceRegistry(true)

	mockKV.EXPECT().Get(defaultNsRegistryKey).Return(nil, kv.ErrNotFound)

	init := NewDynamicInitializer(opts)
	reg, err := init.Init()
	require.NoError(t, err)

	rw, err := reg.Watch()
	require.NoError(t, err)
	rMap := rw.Get()
	require.Nil(t, rMap)

	// Trigger update to add namespace
	value := singleTestValue()
	expectedNsValue := value.Namespaces["testns1"]
	w.Update(value)

	<-rw.C()

	requireNamespaceInRegistry(t, reg, expectedNsValue)

	require.NoError(t, rw.Close())
	require.NoError(t, reg.Close())
}

func TestInitializerAllowEmptyEnabled_ExistingRegistry(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	value := singleTestValue()
	expectedNsValue := value.Namespaces["testns1"]
	w := newTestWatchable(t, value)
	defer w.Close()

	opts, mockKV := newTestSetup(t, ctrl, w)
	opts = opts.SetAllowEmptyInitialNamespaceRegistry(true)

	mockKV.EXPECT().Get(defaultNsRegistryKey).Return(value, nil)

	init := NewDynamicInitializer(opts)
	reg, err := init.Init()
	require.NoError(t, err)

	requireNamespaceInRegistry(t, reg, expectedNsValue)

	require.NoError(t, reg.Close())
}

func requireNamespaceInRegistry(t *testing.T, reg Registry, expectedNsValue *nsproto.NamespaceOptions) {
	rw, err := reg.Watch()
	require.NoError(t, err)
	rMap := rw.Get()
	mds := rMap.Metadatas()
	require.Len(t, mds, 1)
	md := mds[0]
	require.Equal(t, "testns1", md.ID().String())
	require.Equal(t, expectedNsValue.BootstrapEnabled, md.Options().BootstrapEnabled())
	require.Equal(t, expectedNsValue.CleanupEnabled, md.Options().CleanupEnabled())
	require.Equal(t, expectedNsValue.FlushEnabled, md.Options().FlushEnabled())
	require.Equal(t, expectedNsValue.RepairEnabled, md.Options().RepairEnabled())
	require.Equal(t, expectedNsValue.WritesToCommitLog, md.Options().WritesToCommitLog())

	ropts := expectedNsValue.RetentionOptions
	observedRopts := md.Options().RetentionOptions()
	require.Equal(t, ropts.BlockDataExpiry, observedRopts.BlockDataExpiry())
	require.Equal(t, ropts.BlockDataExpiryAfterNotAccessPeriodNanos,
		toNanosInt64(observedRopts.BlockDataExpiryAfterNotAccessedPeriod()))
	require.Equal(t, ropts.BlockSizeNanos, toNanosInt64(observedRopts.BlockSize()))
	require.Equal(t, ropts.BufferFutureNanos, toNanosInt64(observedRopts.BufferFuture()))
	require.Equal(t, ropts.BufferPastNanos, toNanosInt64(observedRopts.BufferPast()))

	latest, found := md.Options().SchemaHistory().GetLatest()
	require.True(t, found)
	require.EqualValues(t, "third", latest.DeployId())

	require.NoError(t, rw.Close())
}

func singleTestValue() *testValue {
	return &testValue{
		version: 1,
		Registry: nsproto.Registry{
			Namespaces: map[string]*nsproto.NamespaceOptions{
				"testns1": &nsproto.NamespaceOptions{
					BootstrapEnabled:  true,
					CleanupEnabled:    true,
					FlushEnabled:      true,
					RepairEnabled:     true,
					WritesToCommitLog: true,
					RetentionOptions: &nsproto.RetentionOptions{
						BlockDataExpiry:                          true,
						BlockDataExpiryAfterNotAccessPeriodNanos: toNanosInt64(time.Minute),
						BlockSizeNanos:                           toNanosInt64(time.Hour * 2),
						RetentionPeriodNanos:                     toNanosInt64(time.Hour * 48),
						BufferFutureNanos:                        toNanosInt64(time.Minute * 10),
						BufferPastNanos:                          toNanosInt64(time.Minute * 15),
					},
					SchemaOptions: testSchemaOptions,
				},
			},
		},
	}
}

type testValue struct {
	nsproto.Registry
	version int
}

func (v *testValue) Unmarshal(msg proto.Message) error {
	reg, ok := msg.(*nsproto.Registry)
	if !ok {
		return fmt.Errorf("incorrect type provided: %T", msg)
	}
	reg.Namespaces = v.Namespaces
	return nil
}

func (v *testValue) Version() int {
	return v.version
}

func (v *testValue) IsNewer(other kv.Value) bool {
	return v.Version() > other.Version()
}

func newTestWatchable(t *testing.T, initValue *testValue) kv.ValueWatchable {
	w := kv.NewValueWatchable()
	if initValue != nil {
		require.NoError(t, w.Update(initValue))
	}
	return w
}

func toNanosInt64(t time.Duration) int64 {
	return xtime.ToNormalizedDuration(t, time.Nanosecond)
}
