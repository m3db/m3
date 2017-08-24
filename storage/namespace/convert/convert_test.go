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

package convert_test

import (
	"testing"

	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/namespace/convert"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/require"
)

var (
	validRetentionOpts = nsproto.RetentionOptions{
		RetentionPeriodMins:                     1200, // 20h
		BlockSizeMins:                           120,  // 2h
		BufferFutureMins:                        12,   // 12m
		BufferPastMins:                          10,   // 10m
		BlockDataExpiry:                         true,
		BlockDataExpiryAfterNotAccessPeriodMins: 30, // 30m
	}

	validNamespaceOpts = nsproto.NamespaceOptions{
		NeedsBootstrap:      true,
		NeedsFlush:          true,
		WritesToCommitLog:   true,
		NeedsFilesetCleanup: true,
		NeedsRepair:         true,
		RetentionOptions:    &validRetentionOpts,
	}

	invalidRetentionOpts = []nsproto.RetentionOptions{
		// block size < buffer past
		nsproto.RetentionOptions{
			RetentionPeriodMins:                     1200, // 20h
			BlockSizeMins:                           2,    // 2m
			BufferFutureMins:                        12,   // 12m
			BufferPastMins:                          10,   // 10m
			BlockDataExpiry:                         true,
			BlockDataExpiryAfterNotAccessPeriodMins: 30, // 30m
		},
		// block size > retention
		nsproto.RetentionOptions{
			RetentionPeriodMins:                     1200, // 20h
			BlockSizeMins:                           1260, // 21h
			BufferFutureMins:                        12,   // 12m
			BufferPastMins:                          10,   // 10m
			BlockDataExpiry:                         true,
			BlockDataExpiryAfterNotAccessPeriodMins: 30, // 30m
		},
	}
)

func TestToRetentionValid(t *testing.T) {
	validOpts := validRetentionOpts
	ropts, err := convert.ToRetention(&validOpts)
	require.NoError(t, err)
	assertEqualRetentions(t, validOpts, ropts)
}

func TestToRetentionInvalid(t *testing.T) {
	for _, opts := range invalidRetentionOpts {
		_, err := convert.ToRetention(&opts)
		require.Error(t, err)
	}
}

func TestToNamespaceValid(t *testing.T) {
	nsOpts, err := convert.ToMetadata("abc", &validNamespaceOpts)
	require.NoError(t, err)
	assertEqualMetadata(t, "abc", validNamespaceOpts, nsOpts)
}

func TestToNamespaceInvalid(t *testing.T) {
	_, err := convert.ToMetadata("", &validNamespaceOpts)
	require.Error(t, err)

	opts := validNamespaceOpts
	opts.RetentionOptions = nil
	_, err = convert.ToMetadata("abc", &opts)
	require.Error(t, err)

	for _, ro := range invalidRetentionOpts {
		opts := validNamespaceOpts
		opts.RetentionOptions = &ro
		_, err = convert.ToMetadata("abc", &opts)
		require.Error(t, err)
	}
}

func TestFromProto(t *testing.T) {
	validRegistry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testns1": &validNamespaceOpts,
			"testns2": &validNamespaceOpts,
		},
	}
	nsMap, err := convert.FromProto(validRegistry)
	require.NoError(t, err)

	md1, err := nsMap.Get(ts.StringID("testns1"))
	require.NoError(t, err)
	assertEqualMetadata(t, "testns1", validNamespaceOpts, md1)

	md2, err := nsMap.Get(ts.StringID("testns2"))
	require.NoError(t, err)
	assertEqualMetadata(t, "testns2", validNamespaceOpts, md2)
}

func TestToProto(t *testing.T) {
	// make ns map
	md1, err := namespace.NewMetadata(ts.StringID("ns1"),
		namespace.NewOptions().SetNeedsBootstrap(true))
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(ts.StringID("ns2"),
		namespace.NewOptions().SetNeedsBootstrap(false))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	// convert to nsproto map
	reg := convert.ToProto(nsMap)
	require.Len(t, reg.Namespaces, 2)

	// NB(prateek): expected/observed are inverted here
	assertEqualMetadata(t, "ns1", *(reg.Namespaces["ns1"]), md1)
	assertEqualMetadata(t, "ns2", *(reg.Namespaces["ns2"]), md2)
}

func assertEqualMetadata(t *testing.T, name string, expected nsproto.NamespaceOptions, observed namespace.Metadata) {
	require.Equal(t, name, observed.ID().String())
	opts := observed.Options()

	require.Equal(t, expected.NeedsBootstrap, opts.NeedsBootstrap())
	require.Equal(t, expected.NeedsFlush, opts.NeedsFlush())
	require.Equal(t, expected.WritesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, expected.NeedsFilesetCleanup, opts.NeedsFilesetCleanup())
	require.Equal(t, expected.NeedsRepair, opts.NeedsRepair())

	assertEqualRetentions(t, *expected.RetentionOptions, opts.RetentionOptions())
}

func assertEqualRetentions(t *testing.T, expected nsproto.RetentionOptions, observed retention.Options) {
	require.Equal(t, expected.RetentionPeriodMins, int64(observed.RetentionPeriod().Minutes()))
	require.Equal(t, expected.BlockSizeMins, int64(observed.BlockSize().Minutes()))
	require.Equal(t, expected.BufferPastMins, int64(observed.BufferPast().Minutes()))
	require.Equal(t, expected.BufferFutureMins, int64(observed.BufferFuture().Minutes()))
	require.Equal(t, expected.BlockDataExpiry, observed.BlockDataExpiry())
	require.Equal(t, expected.BlockDataExpiryAfterNotAccessPeriodMins,
		int64(observed.BlockDataExpiryAfterNotAccessedPeriod().Minutes()))
}
