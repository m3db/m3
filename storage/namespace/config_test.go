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

	"github.com/stretchr/testify/require"

	yaml "gopkg.in/yaml.v2"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
)

func TestRegistryConfig(t *testing.T) {
	var (
		needsBootstrap = false
		config         = &RegistryConfiguration{
			Metadatas: []MetadataConfiguration{
				MetadataConfiguration{
					ID: "abc",
				},
				MetadataConfiguration{
					ID:             "cde",
					NeedsBootstrap: &needsBootstrap,
				},
			},
		}
	)

	reg := config.Registry()
	md, err := reg.Get(ts.StringID("abc"))
	require.NoError(t, err)
	require.Equal(t, config.Metadatas[0].Metadata().ID().String(), md.ID().String())
	require.Equal(t, config.Metadatas[0].Metadata().Options(), md.Options())

	md, err = reg.Get(ts.StringID("cde"))
	require.NoError(t, err)
	require.Equal(t, config.Metadatas[1].Metadata().ID().String(), md.ID().String())
	require.Equal(t, config.Metadatas[1].Metadata().Options(), md.Options())

	_, err = reg.Get(ts.StringID("otherstring"))
	require.Error(t, err)
}

func TestMetadataConfig(t *testing.T) {
	var (
		id                  = "someLongString"
		needsBootstrap      = true
		needsFlush          = false
		writesToCommitLog   = true
		needsFilesetCleanup = false
		needsRepair         = false
		retention           = retention.Configuration{}
		config              = &MetadataConfiguration{
			ID:                  id,
			NeedsBootstrap:      &needsBootstrap,
			NeedsFlush:          &needsFlush,
			WritesToCommitLog:   &writesToCommitLog,
			NeedsFilesetCleanup: &needsFilesetCleanup,
			NeedsRepair:         &needsRepair,
			Retention:           &retention,
		}
	)

	metadata := config.Metadata()
	require.Equal(t, id, metadata.ID().String())

	opts := metadata.Options()
	require.Equal(t, needsBootstrap, opts.NeedsBootstrap())
	require.Equal(t, needsFlush, opts.NeedsFlush())
	require.Equal(t, writesToCommitLog, opts.WritesToCommitLog())
	require.Equal(t, needsFilesetCleanup, opts.NeedsFilesetCleanup())
	require.Equal(t, needsRepair, opts.NeedsRepair())
	require.Equal(t, retention.Options(), opts.RetentionOptions())
}

func TestRegistryConfigFromBytes(t *testing.T) {
	yamlBytes := []byte(`
metadata:
  - id: "testmetrics"
    needsBootstrap: false
    needsFlush: false
    writesToCommitLog: false
    needsFilesetCleanup: false
    needsRepair: false
    retention:
      retentionPeriod: 8h
      blockSize: 2h
      bufferFuture: 10m
      bufferPast: 10m
  - id: "metrics-10s:2d"
    needsBootstrap: true
    needsFlush: true
    writesToCommitLog: true
    needsFilesetCleanup: true
    needsRepair: true
    retention:
      retentionPeriod: 48h
      blockSize: 2h
      bufferFuture: 10m
      bufferPast: 10m
  - id: "metrics-1m:40d"
    needsBootstrap: true
    needsFlush: true
    writesToCommitLog: true
    needsFilesetCleanup: true
    needsRepair: true
    retention:
      retentionPeriod: 960h
      blockSize: 12h
      bufferFuture: 10m
      bufferPast: 10m
`)

	var conf RegistryConfiguration
	require.NoError(t, yaml.Unmarshal(yamlBytes, &conf))

	reg := conf.Registry()
	mds := reg.Metadatas()
	require.Equal(t, 3, len(mds))

	testmetrics := ts.StringID("testmetrics")
	ns, err := reg.Get(testmetrics)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(testmetrics))
	opts := ns.Options()
	require.Equal(t, false, opts.NeedsBootstrap())
	require.Equal(t, false, opts.NeedsFlush())
	require.Equal(t, false, opts.WritesToCommitLog())
	require.Equal(t, false, opts.NeedsFilesetCleanup())
	require.Equal(t, false, opts.NeedsRepair())
	testRetentionOpts := retention.NewOptions().
		SetRetentionPeriod(8 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

	metrics2d := ts.StringID("metrics-10s:2d")
	ns, err = reg.Get(metrics2d)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(metrics2d))
	opts = ns.Options()
	require.Equal(t, true, opts.NeedsBootstrap())
	require.Equal(t, true, opts.NeedsFlush())
	require.Equal(t, true, opts.WritesToCommitLog())
	require.Equal(t, true, opts.NeedsFilesetCleanup())
	require.Equal(t, true, opts.NeedsRepair())
	testRetentionOpts = retention.NewOptions().
		SetRetentionPeriod(48 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

	metrics40d := ts.StringID("metrics-1m:40d")
	ns, err = reg.Get(metrics40d)
	require.NoError(t, err)
	require.True(t, ns.ID().Equal(metrics40d))
	opts = ns.Options()
	require.Equal(t, true, opts.NeedsBootstrap())
	require.Equal(t, true, opts.NeedsFlush())
	require.Equal(t, true, opts.WritesToCommitLog())
	require.Equal(t, true, opts.NeedsFilesetCleanup())
	require.Equal(t, true, opts.NeedsRepair())
	testRetentionOpts = retention.NewOptions().
		SetRetentionPeriod(960 * time.Hour).
		SetBlockSize(12 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute)
	require.True(t, testRetentionOpts.Equal(opts.RetentionOptions()))

}
