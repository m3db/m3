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

package namespace_test

import (
	"testing"
	"time"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	m3test "github.com/m3db/m3/src/x/generated/proto/test"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testSchemaOptions = namespace.GenTestSchemaOptions("mainpkg/main.proto", "testdata")

	toNanos = func(mins int64) int64 {
		return int64(time.Duration(mins) * time.Minute / time.Nanosecond)
	}

	validIndexOpts = nsproto.IndexOptions{
		Enabled:        true,
		BlockSizeNanos: toNanos(600), // 10h
	}

	validRetentionOpts = nsproto.RetentionOptions{
		RetentionPeriodNanos:                     toNanos(1200), // 20h
		BlockSizeNanos:                           toNanos(120),  // 2h
		BufferFutureNanos:                        toNanos(12),   // 12m
		BufferPastNanos:                          toNanos(10),   // 10m
		BlockDataExpiry:                          true,
		BlockDataExpiryAfterNotAccessPeriodNanos: toNanos(30), // 30m
	}

	validExtendedOpts, _ = xtest.NewExtendedOptionsProto("foo")

	validAggregationOpts = nsproto.AggregationOptions{
		Aggregations: []*nsproto.Aggregation{
			{Aggregated: false},
		},
	}

	validNamespaceOpts = []nsproto.NamespaceOptions{
		{
			BootstrapEnabled:      true,
			FlushEnabled:          true,
			WritesToCommitLog:     true,
			CleanupEnabled:        true,
			RepairEnabled:         true,
			CacheBlocksOnRetrieve: &protobuftypes.BoolValue{Value: false},
			RetentionOptions:      &validRetentionOpts,
			SchemaOptions:         testSchemaOptions,
			ExtendedOptions:       validExtendedOpts,
			StagingState:          &nsproto.StagingState{Status: nsproto.StagingStatus_INITIALIZING},
		},
		{
			BootstrapEnabled:  true,
			FlushEnabled:      true,
			WritesToCommitLog: true,
			CleanupEnabled:    true,
			RepairEnabled:     true,
			// Explicitly not setting CacheBlocksOnRetrieve here to test defaulting to true when not set.
			RetentionOptions:   &validRetentionOpts,
			IndexOptions:       &validIndexOpts,
			AggregationOptions: &validAggregationOpts,
		},
	}

	validNamespaceSchemaOpts = []nsproto.NamespaceOptions{
		{
			RetentionOptions: &validRetentionOpts,
			SchemaOptions:    testSchemaOptions,
		},
	}

	invalidRetentionOpts = []nsproto.RetentionOptions{
		// block size < buffer past
		{
			RetentionPeriodNanos:                     toNanos(1200), // 20h
			BlockSizeNanos:                           toNanos(2),    // 2m
			BufferFutureNanos:                        toNanos(12),   // 12m
			BufferPastNanos:                          toNanos(10),   // 10m
			BlockDataExpiry:                          true,
			BlockDataExpiryAfterNotAccessPeriodNanos: toNanos(30), // 30m
		},
		// block size > retention
		{
			RetentionPeriodNanos:                     toNanos(1200), // 20h
			BlockSizeNanos:                           toNanos(1260), // 21h
			BufferFutureNanos:                        toNanos(12),   // 12m
			BufferPastNanos:                          toNanos(10),   // 10m
			BlockDataExpiry:                          true,
			BlockDataExpiryAfterNotAccessPeriodNanos: toNanos(30), // 30m
		},
	}

	invalidAggregationOpts = nsproto.AggregationOptions{
		Aggregations: []*nsproto.Aggregation{
			{
				Aggregated: true,
				Attributes: &nsproto.AggregatedAttributes{
					ResolutionNanos:   -10,
					DownsampleOptions: &nsproto.DownsampleOptions{All: true},
				},
			},
		},
	}
)

func init() {
	namespace.RegisterExtendedOptionsConverter(xtest.TypeURLPrefix, &m3test.PingResponse{}, xtest.ConvertToExtendedOptions)
}

func TestNamespaceToRetentionValid(t *testing.T) {
	validOpts := validRetentionOpts
	ropts, err := namespace.ToRetention(&validOpts)
	require.NoError(t, err)
	assertEqualRetentions(t, validOpts, ropts)
}

func TestNamespaceToRetentionInvalid(t *testing.T) {
	for _, opts := range invalidRetentionOpts {
		_, err := namespace.ToRetention(&opts)
		require.Error(t, err)
	}
}

func TestToMetadataValid(t *testing.T) {
	for _, nsopts := range validNamespaceOpts {
		nsOpts, err := namespace.ToMetadata("abc", &nsopts)
		require.NoError(t, err)
		assertEqualMetadata(t, "abc", nsopts, nsOpts)
	}
}

func TestToMetadataNilIndexOpts(t *testing.T) {
	nsopts := validNamespaceOpts[0]

	nsopts.RetentionOptions.BlockSizeNanos = 7200000000000 / 2
	nsopts.IndexOptions = nil

	nsOpts, err := namespace.ToMetadata("id", &nsopts)
	require.NoError(t, err)
	assert.Equal(t,
		time.Duration(nsopts.RetentionOptions.BlockSizeNanos),
		nsOpts.Options().IndexOptions().BlockSize())
}

func TestToMetadataInvalid(t *testing.T) {
	for _, nsopts := range validNamespaceOpts {
		_, err := namespace.ToMetadata("", &nsopts)
		require.Error(t, err)
	}

	for _, nsopts := range validNamespaceOpts {
		opts := nsopts
		opts.RetentionOptions = nil
		_, err := namespace.ToMetadata("abc", &opts)
		require.Error(t, err)
	}

	for _, nsopts := range validNamespaceOpts {
		for _, ro := range invalidRetentionOpts {
			opts := nsopts
			opts.RetentionOptions = &ro
			_, err := namespace.ToMetadata("abc", &opts)
			require.Error(t, err)
		}
	}
}

func TestFromProto(t *testing.T) {
	validRegistry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testns1": &validNamespaceOpts[0],
			"testns2": &validNamespaceOpts[1],
		},
	}
	nsMap, err := namespace.FromProto(validRegistry)
	require.NoError(t, err)

	md1, err := nsMap.Get(ident.StringID("testns1"))
	require.NoError(t, err)
	assertEqualMetadata(t, "testns1", validNamespaceOpts[0], md1)

	md2, err := nsMap.Get(ident.StringID("testns2"))
	require.NoError(t, err)
	assertEqualMetadata(t, "testns2", validNamespaceOpts[1], md2)
}

func TestToProto(t *testing.T) {
	state, err := namespace.NewStagingState(nsproto.StagingStatus_READY)
	require.NoError(t, err)

	// make ns map
	md1, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().
			SetBootstrapEnabled(true).
			SetStagingState(state))
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(ident.StringID("ns2"),
		namespace.NewOptions().SetBootstrapEnabled(false))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	// convert to nsproto map
	reg, err := namespace.ToProto(nsMap)
	require.NoError(t, err)
	require.Len(t, reg.Namespaces, 2)

	// NB(prateek): expected/observed are inverted here
	assertEqualMetadata(t, "ns1", *(reg.Namespaces["ns1"]), md1)
	assertEqualMetadata(t, "ns2", *(reg.Namespaces["ns2"]), md2)
}

func TestSchemaFromProto(t *testing.T) {
	validRegistry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testns1": &validNamespaceSchemaOpts[0],
		},
	}
	nsMap, err := namespace.FromProto(validRegistry)
	require.NoError(t, err)

	md1, err := nsMap.Get(ident.StringID("testns1"))
	require.NoError(t, err)
	assertEqualMetadata(t, "testns1", validNamespaceSchemaOpts[0], md1)

	require.NotNil(t, md1.Options().SchemaHistory())
	testSchema, found := md1.Options().SchemaHistory().GetLatest()
	require.True(t, found)
	require.NotNil(t, testSchema)
	require.EqualValues(t, "third", testSchema.DeployId())
	require.EqualValues(t, "TestMessage", testSchema.Get().GetName())
}

func TestSchemaToProto(t *testing.T) {
	// make ns map
	testSchemaReg, err := namespace.LoadSchemaHistory(testSchemaOptions)
	require.NoError(t, err)
	md1, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetSchemaHistory(testSchemaReg))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1})
	require.NoError(t, err)

	// convert to nsproto map
	reg, err := namespace.ToProto(nsMap)
	require.NoError(t, err)
	require.Len(t, reg.Namespaces, 1)

	assertEqualMetadata(t, "ns1", *(reg.Namespaces["ns1"]), md1)
	outSchemaReg, err := namespace.LoadSchemaHistory(reg.Namespaces["ns1"].SchemaOptions)
	require.NoError(t, err)
	outSchema, found := outSchemaReg.GetLatest()
	require.True(t, found)
	require.NotNil(t, outSchema)
	require.EqualValues(t, "third", outSchema.DeployId())
	require.EqualValues(t, "TestMessage", outSchema.Get().GetName())
}

func TestToProtoSnapshotEnabled(t *testing.T) {
	md, err := namespace.NewMetadata(
		ident.StringID("ns1"),
		namespace.NewOptions().
			// Don't use default value
			SetSnapshotEnabled(!namespace.NewOptions().SnapshotEnabled()),
	)

	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md})
	require.NoError(t, err)

	reg, err := namespace.ToProto(nsMap)
	require.NoError(t, err)
	require.Len(t, reg.Namespaces, 1)
	require.Equal(t,
		!namespace.NewOptions().SnapshotEnabled(),
		reg.Namespaces["ns1"].SnapshotEnabled,
	)
}

func TestFromProtoSnapshotEnabled(t *testing.T) {
	validRegistry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testns1": &nsproto.NamespaceOptions{
				// Use non-default value
				SnapshotEnabled: !namespace.NewOptions().SnapshotEnabled(),
				// Retention must be set
				RetentionOptions: &validRetentionOpts,
			},
		},
	}
	nsMap, err := namespace.FromProto(validRegistry)
	require.NoError(t, err)

	md, err := nsMap.Get(ident.StringID("testns1"))
	require.NoError(t, err)
	require.Equal(t, !namespace.NewOptions().SnapshotEnabled(), md.Options().SnapshotEnabled())
}

func TestInvalidExtendedOptions(t *testing.T) {
	invalidExtendedOptsBadValue, err := xtest.NewExtendedOptionsProto("foo")
	require.NoError(t, err)
	invalidExtendedOptsBadValue.Value = []byte{1, 2, 3}
	_, err = namespace.ToExtendedOptions(invalidExtendedOptsBadValue)
	assert.Error(t, err)

	invalidExtendedOptsNoConverterForType, err := xtest.NewProtobufAny(&protobuftypes.Int32Value{})
	require.NoError(t, err)
	_, err = namespace.ToExtendedOptions(invalidExtendedOptsNoConverterForType)
	assert.EqualError(t, err, "dynamic ExtendedOptions converter not registered for protobuf type testm3db.io/google.protobuf.Int32Value")

	invalidExtendedOptsConverterFailure, err := xtest.NewExtendedOptionsProto("error")
	require.NoError(t, err)
	_, err = namespace.ToExtendedOptions(invalidExtendedOptsConverterFailure)
	assert.EqualError(t, err, "error in converter")

	invalidExtendedOpts, err := xtest.NewExtendedOptionsProto("invalid")
	require.NoError(t, err)
	_, err = namespace.ToExtendedOptions(invalidExtendedOpts)
	assert.EqualError(t, err, "invalid ExtendedOptions")
}

func TestToAggregationOptions(t *testing.T) {
	aggOpts, err := namespace.ToAggregationOptions(&validAggregationOpts)
	require.NoError(t, err)

	require.Equal(t, 1, len(aggOpts.Aggregations()))

	aggregation := aggOpts.Aggregations()[0]
	require.Equal(t, false, aggregation.Aggregated)
	require.Equal(t, namespace.AggregatedAttributes{}, aggregation.Attributes)
}

func TestToAggregationOptionsInvalid(t *testing.T) {
	_, err := namespace.ToAggregationOptions(&invalidAggregationOpts)
	require.Error(t, err)
}

func TestAggregationOptsToProto(t *testing.T) {
	aggOpts, err := namespace.ToAggregationOptions(&validAggregationOpts)
	require.NoError(t, err)

	// make ns map
	md1, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetAggregationOptions(aggOpts))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1})
	require.NoError(t, err)

	// convert to nsproto map
	reg, err := namespace.ToProto(nsMap)
	require.NoError(t, err)
	require.Len(t, reg.Namespaces, 1)

	nsOpts := *reg.Namespaces["ns1"]

	require.Equal(t, validAggregationOpts, *nsOpts.AggregationOptions)
}

func assertEqualMetadata(t *testing.T, name string, expected nsproto.NamespaceOptions, observed namespace.Metadata) {
	require.Equal(t, name, observed.ID().String())
	opts := observed.Options()

	expectedCacheBlocksOnRetrieve := false
	if expected.CacheBlocksOnRetrieve != nil {
		expectedCacheBlocksOnRetrieve = expected.CacheBlocksOnRetrieve.Value
	}

	require.Equal(t, expected.BootstrapEnabled, opts.BootstrapEnabled())
	require.Equal(t, expected.FlushEnabled, opts.FlushEnabled())
	require.Equal(t, expected.WritesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, expected.CleanupEnabled, opts.CleanupEnabled())
	require.Equal(t, expected.RepairEnabled, opts.RepairEnabled())
	require.Equal(t, expectedCacheBlocksOnRetrieve, opts.CacheBlocksOnRetrieve())
	expectedSchemaReg, err := namespace.LoadSchemaHistory(expected.SchemaOptions)
	require.NoError(t, err)
	require.NotNil(t, expectedSchemaReg)
	require.True(t, expectedSchemaReg.Equal(observed.Options().SchemaHistory()))

	assertEqualRetentions(t, *expected.RetentionOptions, opts.RetentionOptions())
	assertEqualStagingState(t, expected.StagingState, opts.StagingState())
	assertEqualExtendedOpts(t, expected.ExtendedOptions, opts.ExtendedOptions())
}

func assertEqualRetentions(t *testing.T, expected nsproto.RetentionOptions, observed retention.Options) {
	require.Equal(t, expected.RetentionPeriodNanos, observed.RetentionPeriod().Nanoseconds())
	require.Equal(t, expected.BlockSizeNanos, observed.BlockSize().Nanoseconds())
	require.Equal(t, expected.BufferPastNanos, observed.BufferPast().Nanoseconds())
	require.Equal(t, expected.BufferFutureNanos, observed.BufferFuture().Nanoseconds())
	require.Equal(t, expected.BlockDataExpiry, observed.BlockDataExpiry())
	require.Equal(t, expected.BlockDataExpiryAfterNotAccessPeriodNanos,
		observed.BlockDataExpiryAfterNotAccessedPeriod().Nanoseconds())
}

func assertEqualExtendedOpts(t *testing.T, expectedProto *protobuftypes.Any, observed namespace.ExtendedOptions) {
	if expectedProto == nil {
		assert.Nil(t, observed)
		return
	}

	var value = &m3test.PingResponse{}
	err := protobuftypes.UnmarshalAny(expectedProto, value)
	require.NoError(t, err)

	expected, err := xtest.ConvertToExtendedOptions(value)
	require.NoError(t, err)

	assert.Equal(t, expected, observed)
}

func assertEqualStagingState(t *testing.T, expected *nsproto.StagingState, observed namespace.StagingState) {
	if expected == nil {
		assert.Equal(t, namespace.StagingState{}, observed)
		return
	}

	state, err := namespace.NewStagingState(expected.Status)
	require.NoError(t, err)

	require.Equal(t, state, observed)
}
