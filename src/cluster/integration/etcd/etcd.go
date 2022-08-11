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
	"fmt"

	"github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal"
	"github.com/ory/dockertest/v3"
)

type embeddedKV struct {
	etcd *dockerexternal.EtcdCluster
	opts Options
}

// New creates a new EmbeddedKV
func New(opts Options) (EmbeddedKV, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	e, err := dockerexternal.NewEtcd(pool, opts.InstrumentOptions())
	if err != nil {
		return nil, fmt.Errorf("unable to start etcd, err: %v", err)
	}
	return &embeddedKV{
		etcd: e,
		opts: opts,
	}, nil
}

func (e *embeddedKV) Close() error {
	// shutdown and release
	return e.etcd.Close(context.TODO())
}

func (e *embeddedKV) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), e.opts.InitTimeout())
	defer cancel()
	return e.etcd.Setup(ctx)
}

func (e *embeddedKV) Endpoints() []string {
	return e.etcd.Members()
}

func (e *embeddedKV) ConfigServiceClient(fns ...ClientOptFn) (client.Client, error) {
	eopts := etcdclient.NewOptions().
		SetInstrumentOptions(e.opts.InstrumentOptions()).
		SetServicesOptions(services.NewOptions().SetInitTimeout(e.opts.InitTimeout())).
		SetClusters([]etcdclient.Cluster{
			etcdclient.NewCluster().SetZone(e.opts.Zone()).SetEndpoints(e.Endpoints()),
		}).
		SetService(e.opts.ServiceID()).
		SetEnv(e.opts.Environment()).
		SetZone(e.opts.Zone())
	for _, fn := range fns {
		eopts = fn(eopts)
	}
	return etcdclient.NewConfigServiceClient(eopts)
}
