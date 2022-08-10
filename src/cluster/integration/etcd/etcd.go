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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/ory/dockertest/v3"
)

type embeddedKV struct {
	etcd *dockerexternal.EtcdNode
	opts Options
	dir  string
}

// New creates a new EmbeddedKV
func New(opts Options) (EmbeddedKV, error) {
	dir, err := ioutil.TempDir("", opts.Dir())
	if err != nil {
		return nil, err
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("constructing dockertest.Pool for EmbeddedKV: %w", err)
	}
	e, err := dockerexternal.NewEtcd(pool, instrument.NewOptions())
	if err != nil {
		return nil, fmt.Errorf("unable to start etcd, err: %v", err)
	}
	return &embeddedKV{
		etcd: e,
		opts: opts,
		dir:  dir,
	}, nil
}

func (e *embeddedKV) Close() error {
	return e.etcd.Close(context.TODO())
}

func (e *embeddedKV) Start() error {
	timeout := e.opts.InitTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return e.etcd.Setup(ctx)
}

type versionResponse struct {
	Version string `json:"etcdcluster"`
}

func version3Available(endpoint string) bool {
	resp, err := http.Get(endpoint)
	if err != nil {
		return false
	}
	if resp.StatusCode != 200 {
		return false
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var data versionResponse
	err = decoder.Decode(&data)
	if err != nil {
		return false
	}

	return strings.Index(data.Version, "3.") == 0
}

func (e *embeddedKV) Endpoints() []string {
	return []string{e.etcd.Address()}
}

func (e *embeddedKV) ConfigServiceClient(fns ...ClientOptFn) (client.Client, error) {
	eopts := etcdclient.NewOptions().
		SetInstrumentOptions(e.opts.InstrumentOptions()).
		SetServicesOptions(services.NewOptions().SetInitTimeout(e.opts.InitTimeout())).
		SetClusters([]etcdclient.Cluster{
			etcdclient.NewCluster().SetZone(e.opts.Zone()).
				SetEndpoints(e.Endpoints()).
				SetAutoSyncInterval(-1),
		}).
		SetService(e.opts.ServiceID()).
		SetEnv(e.opts.Environment()).
		SetZone(e.opts.Zone())
	for _, fn := range fns {
		eopts = fn(eopts)
	}
	return etcdclient.NewConfigServiceClient(eopts)
}
