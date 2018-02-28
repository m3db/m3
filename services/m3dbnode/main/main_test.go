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

// +build big

package main_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/m3db/m3cluster/integration/etcd"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/kvconfig"
	"github.com/m3db/m3db/services/m3dbnode/config"
	"github.com/m3db/m3db/services/m3dbnode/server"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig tests booting a server using file based configuration.
func TestConfig(t *testing.T) {
	// Embedded kv
	embeddedKV, err := etcd.New(etcd.NewOptions())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, embeddedKV.Close())
	}()
	require.NoError(t, embeddedKV.Start())

	// Create config file
	tmpl, err := template.New("config").Parse(testConfig)
	require.NoError(t, err)

	configFd, cleanup := tempFile(t, "config.yaml")
	defer cleanup()

	logFile, cleanupLogFile := tempFileTouch(t, "m3dbnode.log")
	defer cleanupLogFile()

	configServiceCacheDir, cleanupConfigServiceCacheDir := tempDir(t, "kv")
	defer cleanupConfigServiceCacheDir()

	dataDir, cleanupDataDir := tempDir(t, "data")
	defer cleanupDataDir()

	err = tmpl.Execute(configFd, struct {
		HostID                string
		LogFile               string
		DataDir               string
		ServicePort           string
		ServiceName           string
		ServiceEnv            string
		ServiceZone           string
		ConfigServiceCacheDir string
		EtcdEndpoints         string
	}{
		HostID:                hostID,
		LogFile:               logFile,
		DataDir:               dataDir,
		ServicePort:           strconv.Itoa(int(servicePort)),
		ServiceName:           serviceName,
		ServiceEnv:            serviceEnv,
		ServiceZone:           serviceZone,
		ConfigServiceCacheDir: configServiceCacheDir,
		EtcdEndpoints:         yamlArray(t, embeddedKV.Endpoints()),
	})
	require.NoError(t, err)

	// Setup the placement
	var cfg config.Configuration
	err = xconfig.LoadFile(&cfg, configFd.Name())
	require.NoError(t, err)

	configSvcClient, err := cfg.EnvironmentConfig.Service.NewClient(instrument.NewOptions().
		SetLogger(xlog.NullLogger))
	require.NoError(t, err)

	svcs, err := configSvcClient.Services(services.NewOptions())
	require.NoError(t, err)

	serviceID := services.NewServiceID().
		SetName(serviceName).
		SetEnvironment(serviceEnv).
		SetZone(serviceZone)

	metadata := services.NewMetadata().
		SetPort(servicePort).
		SetLivenessInterval(time.Minute).
		SetHeartbeatInterval(10 * time.Second)

	err = svcs.SetMetadata(serviceID, metadata)
	require.NoError(t, err)

	placementOpts := placement.NewOptions().
		SetValidZone(serviceZone)
	placementSvc, err := svcs.PlacementService(serviceID, placementOpts)
	require.NoError(t, err)

	instance := placement.NewInstance().
		SetID(hostID).
		SetEndpoint(endpoint("127.0.0.1", servicePort)).
		SetPort(servicePort).
		SetRack("local").
		SetWeight(1).
		SetZone(serviceZone)
	instances := []placement.Instance{instance}
	shards := 256
	replicas := 1
	_, err = placementSvc.BuildInitialPlacement(instances, shards, replicas)
	require.NoError(t, err)

	// Setup the namespace
	ns, err := newNamespaceProtoValue(namespaceID)
	require.NoError(t, err)

	kvStore, err := configSvcClient.KV()
	require.NoError(t, err)

	_, err = kvStore.Set(kvconfig.NamespacesKey, ns)
	require.NoError(t, err)

	// Run server
	var (
		interruptCh = make(chan error, 1)
		bootstrapCh = make(chan struct{}, 1)
		serverWg    sync.WaitGroup
	)
	serverWg.Add(1)
	go func() {
		server.Run(server.RunOptions{
			ConfigFile:  configFd.Name(),
			BootstrapCh: bootstrapCh,
			InterruptCh: interruptCh,
		})
		serverWg.Done()
	}()

	// Wait for bootstrap
	<-bootstrapCh

	// Create client, read and write some data
	// NB(r): Make sure client config points to the root config
	// service since we're going to instantiate the client configuration
	// just by itself.
	cfg.Client.EnvironmentConfig.Service = cfg.EnvironmentConfig.Service

	cli, err := cfg.Client.NewClient(client.ConfigurationParameters{})
	require.NoError(t, err)

	session, err := cli.DefaultSession()
	require.NoError(t, err)

	defer session.Close()

	start := time.Now().Add(-time.Minute)
	values := []struct {
		value float64
		at    time.Time
		unit  xtime.Unit
	}{
		{value: 1.0, at: start, unit: xtime.Second},
		{value: 2.0, at: start.Add(1 * time.Second), unit: xtime.Second},
		{value: 3.0, at: start.Add(2 * time.Second), unit: xtime.Second},
	}

	for _, v := range values {
		err := session.Write(ident.StringID(namespaceID), ident.StringID("foo"), v.at, v.value, v.unit, nil)
		require.NoError(t, err)
	}

	// Account for first value inserted at xtime.Second precision
	fetchStart := start.Truncate(time.Second)

	// Account for last value being inserted at xtime.Second and
	// the "end" param to fetch being exclusive
	fetchEnd := values[len(values)-1].at.Truncate(time.Second).Add(time.Nanosecond)

	iter, err := session.Fetch(ident.StringID(namespaceID), ident.StringID("foo"), fetchStart, fetchEnd)
	require.NoError(t, err)

	for _, v := range values {
		require.True(t, iter.Next())
		dp, unit, _ := iter.Current()
		assert.Equal(t, v.value, dp.Value)
		// Account for xtime.Second precision on values going in
		expectAt := v.at.Truncate(time.Second)
		assert.Equal(t, expectAt, dp.Timestamp)
		assert.Equal(t, v.unit, unit)
	}

	// Wait for server to stop
	interruptCh <- fmt.Errorf("test complete")
	serverWg.Wait()
}
