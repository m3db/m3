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

package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestOverrideConfiguration(t *testing.T) {
	configStr := `
namespaces:
  placement: p
  metadata: m
`

	var cfg OverrideConfiguration
	err := yaml.Unmarshal([]byte(configStr), &cfg)
	require.NoError(t, err)
	opts := cfg.NewOptions()
	require.Equal(t, "p", opts.NamespaceOptions().PlacementNamespace())
	require.Equal(t, "m", opts.NamespaceOptions().MetadataNamespace())
}

func TestNamespaceConfiguration(t *testing.T) {
	configStr := `
placement: p
metadata: m
`

	var cfg NamespacesConfiguration
	err := yaml.Unmarshal([]byte(configStr), &cfg)
	require.NoError(t, err)
	opts := cfg.NewOptions()
	require.Equal(t, "p", opts.PlacementNamespace())
	require.Equal(t, "m", opts.MetadataNamespace())
}

func TestConfig(t *testing.T) {
	cf := Configuration{}
	require.Equal(t, defaultInitTimeout, cf.NewOptions().InitTimeout())

	sec := time.Second
	cf = Configuration{InitTimeout: &sec}
	require.Equal(t, time.Second, cf.NewOptions().InitTimeout())
}

func TestElectionOptions(t *testing.T) {
	configStr := `
leaderTimeout: 5s
resignTimeout: 10s
TTLSeconds: 15
`

	var cfg ElectionConfiguration
	err := yaml.Unmarshal([]byte(configStr), &cfg)
	require.NoError(t, err)
	opts := cfg.NewOptions()
	require.Equal(t, time.Second*5, opts.LeaderTimeout())
	require.Equal(t, time.Second*10, opts.ResignTimeout())
	require.Equal(t, 15, opts.TTLSecs())
}

func TestServiceID(t *testing.T) {
	configStr := `
name: sampleName
environment: sampleEnvironment
zone: sampleZone
`

	var cfg ServiceIDConfiguration
	err := yaml.Unmarshal([]byte(configStr), &cfg)
	require.NoError(t, err)
	sID := cfg.NewServiceID()
	require.Equal(t, "sampleName", sID.Name())
	require.Equal(t, "sampleEnvironment", sID.Environment())
	require.Equal(t, "sampleZone", sID.Zone())
}
