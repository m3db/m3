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
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

var (
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

	validNamespaceOpts = []nsproto.NamespaceOptions{
		nsproto.NamespaceOptions{
			BootstrapEnabled:  true,
			FlushEnabled:      true,
			WritesToCommitLog: true,
			CleanupEnabled:    true,
			RepairEnabled:     true,
			RetentionOptions:  &validRetentionOpts,
			SchemaOptions:     namespace.GenTestSchemaOptions("schematest"),
		},
		nsproto.NamespaceOptions{
			BootstrapEnabled:  true,
			FlushEnabled:      true,
			WritesToCommitLog: true,
			CleanupEnabled:    true,
			RepairEnabled:     true,
			RetentionOptions:  &validRetentionOpts,
			IndexOptions:      &validIndexOpts,
		},
	}

	validNamespaceSchemaOpts = []nsproto.NamespaceOptions{
		nsproto.NamespaceOptions{
			RetentionOptions: &validRetentionOpts,
			SchemaOptions:    namespace.GenTestSchemaOptions("schematest"),
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
)

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
	reg := namespace.ToProto(nsMap)
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
	testSchemaReg, err := namespace.LoadSchemaHistory(namespace.GenTestSchemaOptions("schematest"))
	require.NoError(t, err)
	md1, err := namespace.NewMetadata(ident.StringID("ns1"),
		namespace.NewOptions().SetSchemaHistory(testSchemaReg))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1})
	require.NoError(t, err)

	// convert to nsproto map
	reg := namespace.ToProto(nsMap)
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

	reg := namespace.ToProto(nsMap)
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

func assertEqualMetadata(t *testing.T, name string, expected nsproto.NamespaceOptions, observed namespace.Metadata) {
	require.Equal(t, name, observed.ID().String())
	opts := observed.Options()

	require.Equal(t, expected.BootstrapEnabled, opts.BootstrapEnabled())
	require.Equal(t, expected.FlushEnabled, opts.FlushEnabled())
	require.Equal(t, expected.WritesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, expected.CleanupEnabled, opts.CleanupEnabled())
	require.Equal(t, expected.RepairEnabled, opts.RepairEnabled())
	expectedSchemaReg, err := namespace.LoadSchemaHistory(expected.SchemaOptions)
	require.NoError(t, err)
	require.NotNil(t, expectedSchemaReg)
	require.True(t, expectedSchemaReg.Equal(observed.Options().SchemaHistory()))

	assertEqualRetentions(t, *expected.RetentionOptions, opts.RetentionOptions())
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
