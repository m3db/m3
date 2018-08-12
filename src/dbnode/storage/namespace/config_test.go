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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestRegistryConfig(t *testing.T) {
	var (
		bootstrapEnabled = false
		config           = &MapConfiguration{
			Metadatas: []MetadataConfiguration{
				MetadataConfiguration{
					ID: "abc",
					Retention: retention.Configuration{
						BlockSize:       time.Hour,
						RetentionPeriod: time.Hour,
						BufferFuture:    time.Minute,
						BufferPast:      time.Minute,
					},
				},
				MetadataConfiguration{
					ID:               "cde",
					BootstrapEnabled: &bootstrapEnabled,
					Retention: retention.Configuration{
						BlockSize:       time.Hour,
						RetentionPeriod: time.Hour,
						BufferFuture:    time.Minute,
						BufferPast:      time.Minute,
					},
					Index: IndexConfiguration{
						Enabled:   true,
						BlockSize: time.Hour,
					},
				},
			},
		}
	)

	nsMap, err := config.Map()
	require.NoError(t, err)
	md, err := nsMap.Get(ident.StringID("abc"))
	require.NoError(t, err)
	mdd, err := config.Metadatas[0].Metadata()
	require.NoError(t, err)
	require.Equal(t, mdd.ID().String(), md.ID().String())
	require.Equal(t, mdd.Options(), md.Options())

	md, err = nsMap.Get(ident.StringID("cde"))
	require.NoError(t, err)
	mdd, err = config.Metadatas[1].Metadata()
	require.NoError(t, err)
	require.Equal(t, mdd.ID().String(), md.ID().String())
	require.Equal(t, mdd.Options(), md.Options())

	_, err = nsMap.Get(ident.StringID("otherstring"))
	require.Error(t, err)
}

func TestMetadataConfig(t *testing.T) {
	var (
		id                = "someLongString"
		bootstrapEnabled  = true
		flushEnabled      = false
		writesToCommitLog = true
		cleanupEnabled    = false
		repairEnabled     = false
		retention         = retention.Configuration{
			BlockSize:       time.Hour,
			RetentionPeriod: time.Hour,
			BufferFuture:    time.Minute,
			BufferPast:      time.Minute,
		}
		index = IndexConfiguration{
			Enabled:   true,
			BlockSize: time.Hour,
		}
		config = &MetadataConfiguration{
			ID:                id,
			BootstrapEnabled:  &bootstrapEnabled,
			FlushEnabled:      &flushEnabled,
			WritesToCommitLog: &writesToCommitLog,
			CleanupEnabled:    &cleanupEnabled,
			RepairEnabled:     &repairEnabled,
			Retention:         retention,
			Index:             index,
		}
	)

	metadata, err := config.Metadata()
	require.NoError(t, err)
	require.Equal(t, id, metadata.ID().String())

	opts := metadata.Options()
	require.Equal(t, bootstrapEnabled, opts.BootstrapEnabled())
	require.Equal(t, flushEnabled, opts.FlushEnabled())
	require.Equal(t, writesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, cleanupEnabled, opts.CleanupEnabled())
	require.Equal(t, repairEnabled, opts.RepairEnabled())
	require.Equal(t, retention.Options(), opts.RetentionOptions())
	require.Equal(t, index.Options(), opts.IndexOptions())
}

func TestRegistryConfigFromBytes(t *testing.T) {
	yamlBytes := []byte(`
metadatas:
  - id: "testmetrics"
    bootstrapEnabled: false
    flushEnabled: false
    writesToCommitLog: false
    cleanupEnabled: false
    repairEnabled: false
    retention:
      retentionPeriod: 8h
      blockSize: 2h
      bufferFuture: 10m
      bufferPast: 10m
  - id: "metrics-10s:2d"
    bootstrapEnabled: true
    flushEnabled: true
    writesToCommitLog: true
    cleanupEnabled: true
    repairEnabled: true
    retention:
      retentionPeriod: 48h
      blockSize: 2h
      bufferFuture: 10m
      bufferPast: 10m
  - id: "metrics-1m:40d"
    bootstrapEnabled: true
    flushEnabled: true
    writesToCommitLog: true
    cleanupEnabled: true
    repairEnabled: true
    retention:
      retentionPeriod: 960h
      blockSize: 12h
      bufferFuture: 10m
      bufferPast: 10m
    index:
      enabled: true
      blockSize: 24h
`)

	var conf MapConfiguration
	require.NoError(t, yaml.Unmarshal(yamlBytes, &conf))

	nsMap, err := conf.Map()
	require.NoError(t, err)
	mds := nsMap.Metadatas()
	require.Equal(t, 3, len(mds))

	testmetrics := ident.StringID("testmetrics")
	ns, err := nsMap.Get(testmetrics)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(testmetrics))
	opts := ns.Options()
	require.Equal(t, false, opts.BootstrapEnabled())
	require.Equal(t, false, opts.FlushEnabled())
	require.Equal(t, false, opts.WritesToCommitLog())
	require.Equal(t, false, opts.CleanupEnabled())
	require.Equal(t, false, opts.RepairEnabled())
	require.Equal(t, false, opts.IndexOptions().Enabled())
	testRetentionOpts := retention.NewOptions().
		SetRetentionPeriod(8 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

	metrics2d := ident.StringID("metrics-10s:2d")
	ns, err = nsMap.Get(metrics2d)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(metrics2d))
	opts = ns.Options()
	require.Equal(t, true, opts.BootstrapEnabled())
	require.Equal(t, true, opts.FlushEnabled())
	require.Equal(t, true, opts.WritesToCommitLog())
	require.Equal(t, true, opts.CleanupEnabled())
	require.Equal(t, true, opts.RepairEnabled())
	require.Equal(t, false, opts.IndexOptions().Enabled())
	testRetentionOpts = retention.NewOptions().
		SetRetentionPeriod(48 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

	metrics40d := ident.StringID("metrics-1m:40d")
	ns, err = nsMap.Get(metrics40d)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(metrics40d))
	opts = ns.Options()
	require.Equal(t, true, opts.BootstrapEnabled())
	require.Equal(t, true, opts.FlushEnabled())
	require.Equal(t, true, opts.WritesToCommitLog())
	require.Equal(t, true, opts.CleanupEnabled())
	require.Equal(t, true, opts.RepairEnabled())
	require.Equal(t, true, opts.IndexOptions().Enabled())
	require.Equal(t, 24*time.Hour, opts.IndexOptions().BlockSize())
	testRetentionOpts = retention.NewOptions().
		SetRetentionPeriod(960 * time.Hour).
		SetBlockSize(12 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

}
