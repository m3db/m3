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

package etcd

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3x/config"

	"github.com/stretchr/testify/require"
)

const testConfig = `
    env: env1
    zone: z1
    service: service1
    cacheDir: /tmp/cache.json
    etcdClusters:
        - zone: z1
          endpoints:
              - etcd1:2379
              - etcd2:2379
        - zone: z2
          endpoints:
              - etcd3:2379
              - etcd4:2379
          cert: foo.crt.pem
          key: foo.key.pem
        - zone: z3
          endpoints:
              - etcd5:2379
              - etcd6:2379
          cert: foo.crt.pem
          key: foo.key.pem
          ca: foo_ca.pem
    m3sd:
        initTimeout: 10s
`

func TestConfig(t *testing.T) {
	fname := writeFile(t, testConfig)
	defer os.Remove(fname)

	var cfg Configuration
	config.LoadFile(&cfg, fname)

	require.Equal(t, "env1", cfg.Env)
	require.Equal(t, "z1", cfg.Zone)
	require.Equal(t, "service1", cfg.Service)
	require.Equal(t, "/tmp/cache.json", cfg.CacheDir)
	require.Equal(t, []ClusterConfig{
		ClusterConfig{Zone: "z1", Endpoints: []string{"etcd1:2379", "etcd2:2379"}},
		ClusterConfig{
			Zone:      "z2",
			Endpoints: []string{"etcd3:2379", "etcd4:2379"},
			Cert:      "foo.crt.pem",
			Key:       "foo.key.pem",
		},
		ClusterConfig{
			Zone:      "z3",
			Endpoints: []string{"etcd5:2379", "etcd6:2379"},
			Cert:      "foo.crt.pem",
			Key:       "foo.key.pem",
			CA:        "foo_ca.pem",
		},
	}, cfg.ETCDClusters)
	require.Equal(t, 10*time.Second, cfg.SDConfig.InitTimeout)
}

// nolint: unparam
func writeFile(t *testing.T, contents string) string {
	f, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	defer f.Close()

	_, err = f.Write([]byte(contents))
	require.NoError(t, err)

	return f.Name()
}
