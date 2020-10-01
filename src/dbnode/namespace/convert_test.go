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
	"errors"
	"testing"
	"time"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/ident"

	"github.com/gogo/protobuf/proto"
	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testSchemaOptions = namespace.GenTestSchemaOptions("mainpkg/main.proto", "testdata")

	testTypeUrlPrefix = "testm3db.io/"

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

	validExtendedOpts = newTestExtendedOptionsProto(1)

	validAggregationOpts = nsproto.AggregationOptions{
		Aggregations: []*nsproto.Aggregation{
			{Aggregated: false},
		},
	}

	validNamespaceOpts = []nsproto.NamespaceOptions{
		nsproto.NamespaceOptions{
			BootstrapEnabled:      true,
			FlushEnabled:          true,
			WritesToCommitLog:     true,
			CleanupEnabled:        true,
			RepairEnabled:         true,
			CacheBlocksOnRetrieve: &protobuftypes.BoolValue{Value: false},
			RetentionOptions:      &validRetentionOpts,
			SchemaOptions:         testSchemaOptions,
			ExtendedOptions:       validExtendedOpts,
		},
		nsproto.NamespaceOptions{
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
		nsproto.NamespaceOptions{
			RetentionOptions: &validRetentionOpts,
			SchemaOptions:    testSchemaOptions,
		},
	}

	invalidRetentionOpts = []nsproto.RetentionOptions{
		// block size < buffer past
		nsproto.RetentionOptions{
			RetentionPeriodNanos:                     toNanos(1200), // 20h
			BlockSizeNanos:                           toNanos(2),    // 2m
			BufferFutureNanos:                        toNanos(12),   // 12m
			BufferPastNanos:                          toNanos(10),   // 10m
			BlockDataExpiry:                          true,
			BlockDataExpiryAfterNotAccessPeriodNanos: toNanos(30), // 30m
		},
		// block size > retention
		nsproto.RetentionOptions{
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

type testExtendedOptions struct {
	enabled        bool
	blockSizeNanos int64
}

func newTestExtendedOptionsProto(value int64) *protobuftypes.Any {
	// NB: using some arbitrary custom protobuf message so that we don't have to introduce any new protobuf just for tests.
	msg := &nsproto.IndexOptions{Enabled: true, BlockSizeNanos: value}
	serializedMsg, _ := proto.Marshal(msg)

	return &protobuftypes.Any{
		TypeUrl: testTypeUrlPrefix + proto.MessageName(msg),
		Value:   serializedMsg,
	}
}

func (o *testExtendedOptions) ToProto() (proto.Message, string) {
	return &nsproto.IndexOptions{Enabled: o.enabled, BlockSizeNanos: o.blockSizeNanos}, testTypeUrlPrefix
}

func (o *testExtendedOptions) Validate() error {
	if o.blockSizeNanos == 44 {
		return errors.New("invalid ExtendedOptions")
	}
	return nil
}

func convertProtobufIndexOptions(message proto.Message) (namespace.ExtendedOptions, error) {
	value := message.(*nsproto.IndexOptions)
	if value.BlockSizeNanos == 55 {
		return nil, errors.New("error in converter")
	}
	return &testExtendedOptions{value.Enabled, value.BlockSizeNanos}, nil
}

func init() {
	namespace.RegisterExtendedOptionsConverter(testTypeUrlPrefix, &nsproto.IndexOptions{}, convertProtobufIndexOptions)
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

func TestToNamespaceValid(t *testing.T) {
	for _, nsopts := range validNamespaceOpts {
		nsOpts, err := namespace.ToMetadata("abc", &nsopts)
		require.NoError(t, err)
		assertEqualMetadata(t, "abc", nsopts, nsOpts)
	}
}

func TestToNamespaceInvalid(t *testing.T) {
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
	// make ns map
	md1, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetBootstrapEnabled(true))
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
	invalidExtendedOptsBadValue := &protobuftypes.Any{
		TypeUrl: testTypeUrlPrefix + proto.MessageName(&protobuftypes.StringValue{Value: "foo"}),
		Value:   []byte{1, 2, 3},
	}
	_, err := namespace.ToExtendedOptions(invalidExtendedOptsBadValue)
	assert.Error(t, err)

	msg := &protobuftypes.Int32Value{}
	serializedMsg, _ := proto.Marshal(msg)
	invalidExtendedOptsNoConverterForType := &protobuftypes.Any{
		TypeUrl: testTypeUrlPrefix + proto.MessageName(msg),
		Value:   serializedMsg,
	}
	_, err = namespace.ToExtendedOptions(invalidExtendedOptsNoConverterForType)
	assert.Equal(t, errors.New("dynamic ExtendedOptions converter not registered for protobuf type testm3db.io/google.protobuf.Int32Value"), err)

	invalidExtendedOptsConverterFailure := newTestExtendedOptionsProto(55)
	_, err = namespace.ToExtendedOptions(invalidExtendedOptsConverterFailure)
	assert.Equal(t, errors.New("error in converter"), err)

	invalidExtendedOpts := newTestExtendedOptionsProto(44)
	_, err = namespace.ToExtendedOptions(invalidExtendedOpts)
	assert.Equal(t, errors.New("invalid ExtendedOptions"), err)
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

	expectedCacheBlocksOnRetrieve := true
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

	var value = &nsproto.IndexOptions{}
	err := protobuftypes.UnmarshalAny(expectedProto, value)
	require.NoError(t, err)

	expected, err := convertProtobufIndexOptions(value)
	require.NoError(t, err)

	assert.Equal(t, expected, observed)
}
