// +build big
//
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

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/ident"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	hostID                 = "m3dbtest01"
	serviceName            = "m3dbnode_test"
	serviceEnv             = "test"
	serviceZone            = "local"
	namespaceID            = "metrics"
	lpURL                  = "http://0.0.0.0:2380"
	lcURL                  = "http://0.0.0.0:2379"
	apURL                  = "http://localhost:2380"
	acURL                  = "http://localhost:2379"
	etcdEndpoint           = acURL
	initialClusterHostID   = hostID
	initialClusterEndpoint = apURL
)

var (
	// NB: access through nextServicePort
	_servicePort uint32 = 9000
)

func nextServicePort() uint32 {
	return atomic.AddUint32(&_servicePort, 1000)
}

type cleanup func()

func tempFile(t *testing.T, name string) (*os.File, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	return fd, func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fname))
	}
}

func tempFileTouch(t *testing.T, name string) (string, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	require.NoError(t, fd.Close())
	return fname, func() {
		assert.NoError(t, os.Remove(fname))
	}
}

func tempDir(t *testing.T, name string) (string, cleanup) {
	dir, err := ioutil.TempDir("", name)
	require.NoError(t, err)
	return dir, func() {
		assert.NoError(t, os.RemoveAll(dir))
	}
}

// yamlArray returns a JSON array which is valid YAML, hehe..
func yamlArray(t *testing.T, values []string) string {
	buff := bytes.NewBuffer(nil)
	err := json.NewEncoder(buff).Encode(values)
	require.NoError(t, err)
	return strings.TrimSpace(buff.String())
}

func endpoint(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func newNamespaceProtoValue(id string) (proto.Message, error) {
	return newNamespaceWithIndexProtoValue(id, false)
}

func newNamespaceWithIndexProtoValue(id string, indexEnabled bool) (proto.Message, error) {
	md, err := namespace.NewMetadata(
		ident.StringID(id),
		namespace.NewOptions().
			SetBootstrapEnabled(true).
			SetCleanupEnabled(true).
			SetFlushEnabled(true).
			SetRepairEnabled(true).
			SetWritesToCommitLog(true).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(1*time.Hour).
					SetRetentionPeriod(24*time.Hour)).
			SetIndexOptions(
				namespace.NewIndexOptions().
					SetBlockSize(6*time.Hour).
					SetEnabled(indexEnabled)))
	if err != nil {
		return nil, err
	}
	nsMap, err := namespace.NewMap([]namespace.Metadata{md})
	if err != nil {
		return nil, err
	}

	registry, err := namespace.ToProto(nsMap)
	if err != nil {
		return nil, err
	}

	return registry, nil
}

// waitUntilAllShardsAreAvailable continually polls the session checking to see if the topology.Map
// that the session is currently storing contains a non-zero number of host shard sets, and if so,
// makes sure that all their shard states are Available.
func waitUntilAllShardsAreAvailable(t *testing.T, session client.AdminSession) {
outer:
	for {
		time.Sleep(10 * time.Millisecond)

		topoMap, err := session.TopologyMap()
		require.NoError(t, err)

		var (
			hostShardSets = topoMap.HostShardSets()
		)

		if len(hostShardSets) == 0 {
			// We haven't received an actual topology yet.
			continue
		}

		for _, hostShardSet := range hostShardSets {
			for _, hostShard := range hostShardSet.ShardSet().All() {
				if hostShard.State() != shard.Available {
					continue outer
				}
			}
		}

		break
	}
}
