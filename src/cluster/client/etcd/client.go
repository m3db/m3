// Copyright (c) 2016 Uber Technologies, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	etcdKV "github.com/m3db/m3cluster/kv/etcd"
	"github.com/m3db/m3cluster/services"
	sdClient "github.com/m3db/m3cluster/services/client"
	etcdHeartbeat "github.com/m3db/m3cluster/services/heartbeat/etcd"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

const (
	keySeparator    = "/"
	cacheFileFormat = "%s-%s.json"
	kvPrefix        = "_kv/"
)

type newClientFn func(endpoints []string) (*clientv3.Client, error)

// NewConfigServiceClient returns a ConfigServiceClient
func NewConfigServiceClient(opts Options) (client.Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	scope := opts.InstrumentOptions().
		MetricsScope().
		Tagged(map[string]string{"app_id": opts.AppID()})

	return &csclient{
		opts:    opts,
		kvScope: scope.Tagged(map[string]string{"config_service": "kv"}),
		sdScope: scope.Tagged(map[string]string{"config_service": "sd"}),
		hbScope: scope.Tagged(map[string]string{"config_service": "hb"}),
		clis:    make(map[string]*clientv3.Client),
		logger:  opts.InstrumentOptions().Logger(),
		newFn:   newClient,
	}, nil
}

type csclient struct {
	sync.RWMutex
	clis map[string]*clientv3.Client

	opts    Options
	kvScope tally.Scope
	sdScope tally.Scope
	hbScope tally.Scope
	logger  xlog.Logger
	newFn   newClientFn

	sdOnce sync.Once
	sd     services.Services
	sdErr  error

	kvOnce sync.Once
	kv     kv.Store
	kvErr  error
}

func (c *csclient) Services() (services.Services, error) {
	c.sdOnce.Do(func() {
		c.sd, c.sdErr = c.newServices()
	})

	return c.sd, c.sdErr
}

func (c *csclient) KV() (kv.Store, error) {
	c.kvOnce.Do(func() {
		c.kv, c.kvErr = c.newKVStore()
	})

	return c.kv, c.kvErr
}

func (c *csclient) newServices() (services.Services, error) {
	return sdClient.NewServices(sdClient.NewOptions().
		SetInitTimeout(c.opts.ServiceInitTimeout()).
		SetHeartbeatGen(c.heartbeatGen()).
		SetKVGen(c.kvGen(etcdKV.NewOptions().
			SetInstrumentsOptions(instrument.NewOptions().
				SetLogger(c.logger).
				SetMetricsScope(c.kvScope),
			)),
		).
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.sdScope),
		),
	)
}

func (c *csclient) newKVStore() (kv.Store, error) {
	env := c.opts.Env()
	kvGen := c.kvGen(etcdKV.NewOptions().
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.kvScope),
		).
		SetPrefix(prefix(env)),
	)
	return kvGen(c.opts.Zone())
}

func (c *csclient) kvGen(kvOpts etcdKV.Options) sdClient.KVGen {
	return sdClient.KVGen(
		func(zone string) (kv.Store, error) {
			cli, err := c.etcdClientGen(zone)
			if err != nil {
				return nil, err
			}

			return etcdKV.NewStore(
				cli.KV,
				cli.Watcher,
				kvOpts.SetCacheFilePath(cacheFileForZone(c.opts.CacheDir(), kvOpts.ApplyPrefix(c.opts.AppID()), zone)),
			)
		},
	)
}

func (c *csclient) heartbeatGen() sdClient.HeartbeatGen {
	return sdClient.HeartbeatGen(
		func(sid services.ServiceID) (services.HeartbeatService, error) {
			cli, err := c.etcdClientGen(sid.Zone())
			if err != nil {
				return nil, err
			}

			opts := etcdHeartbeat.NewOptions().
				SetInstrumentsOptions(
					instrument.NewOptions().
						SetLogger(c.logger).
						SetMetricsScope(c.hbScope),
				).SetServiceID(sid)
			return etcdHeartbeat.NewStore(cli, opts)
		},
	)
}

func (c *csclient) etcdClientGen(zone string) (*clientv3.Client, error) {
	c.Lock()
	defer c.Unlock()

	cli, ok := c.clis[zone]
	if ok {
		return cli, nil
	}

	cluster, ok := c.opts.ClusterForZone(zone)
	if !ok {
		return nil, fmt.Errorf("no etcd cluster found for zone %s", zone)
	}

	cli, err := c.newFn(cluster.Endpoints())
	if err != nil {
		return nil, err
	}

	c.clis[zone] = cli
	return cli, nil
}

func newClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{Endpoints: endpoints})
}

func cacheFileForZone(cacheDir, appID, zone string) string {
	if cacheDir == "" || appID == "" || zone == "" {
		return ""
	}
	return filepath.Join(cacheDir, fileName(appID, zone))
}

func fileName(appID, zone string) string {
	cacheFileName := fmt.Sprintf(cacheFileFormat, appID, zone)

	return strings.Replace(cacheFileName, string(os.PathSeparator), "_", -1)
}

func prefix(env string) string {
	res := kvPrefix
	if env != "" {
		res = concat(res, concat(env, keySeparator))
	}
	return res
}

func concat(a, b string) string {
	return fmt.Sprintf("%s%s", a, b)
}
