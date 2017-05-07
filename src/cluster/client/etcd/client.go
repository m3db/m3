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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	etcdkv "github.com/m3db/m3cluster/kv/etcd"
	"github.com/m3db/m3cluster/services"
	sdclient "github.com/m3db/m3cluster/services/client"
	etcdheartbeat "github.com/m3db/m3cluster/services/heartbeat/etcd"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"

	"github.com/coreos/etcd/clientv3"
	"github.com/uber-go/tally"
)

const (
	internalPrefix     = "_"
	cacheFileSeparator = "_"
	cacheFileSuffix    = ".json"
	// TODO deprecate this once all keys are migrated to per service namespace
	kvPrefix = "_kv"
)

var errInvalidNamespace = errors.New("invalid namespace")

type newClientFn func(endpoints []string) (*clientv3.Client, error)

// NewConfigServiceClient returns a ConfigServiceClient
func NewConfigServiceClient(opts Options) (client.Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	scope := opts.InstrumentOptions().
		MetricsScope().
		Tagged(map[string]string{"service": opts.Service()})

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

	txnOnce sync.Once
	txn     kv.TxnStore
	txnErr  error
}

func (c *csclient) Services() (services.Services, error) {
	c.createServices()

	return c.sd, c.sdErr
}

func (c *csclient) KV() (kv.Store, error) {
	return c.createTxnStore(kvPrefix)
}

func (c *csclient) Txn() (kv.TxnStore, error) {
	return c.createTxnStore(kvPrefix)

}

func (c *csclient) TxnStore(namespace string) (kv.TxnStore, error) {
	if err := validateTopLevelNamespace(namespace); err != nil {
		return nil, err
	}

	return c.createTxnStore(namespace)
}

func (c *csclient) Store(namespace string) (kv.Store, error) {
	return c.TxnStore(namespace)
}

func (c *csclient) createServices() {
	c.sdOnce.Do(func() {
		c.sd, c.sdErr = sdclient.NewServices(c.opts.ServiceDiscoveryConfig().NewOptions().
			SetHeartbeatGen(c.heartbeatGen()).
			SetKVGen(c.kvGen()).
			SetInstrumentsOptions(instrument.NewOptions().
				SetLogger(c.logger).
				SetMetricsScope(c.sdScope),
			),
		)
	})
}

func (c *csclient) createTxnStore(namespace string) (kv.TxnStore, error) {
	c.txnOnce.Do(func() {
		c.txn, c.txnErr = c.txnGen(c.opts.Zone(), namespace, c.opts.Env())
	})
	return c.txn, c.txnErr
}

func (c *csclient) kvGen() sdclient.KVGen {
	return sdclient.KVGen(func(zone string) (kv.Store, error) {
		return c.txnGen(zone)
	})
}

func (c *csclient) newkvOptions(zone string, namespaces ...string) etcdkv.Options {
	opts := etcdkv.NewOptions().
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.kvScope)).
		SetCacheFileFn(c.cacheFileFn(zone))

	for _, namespace := range namespaces {
		if namespace == "" {
			continue
		}
		opts = opts.SetPrefix(opts.ApplyPrefix(namespace))
	}
	return opts
}

func (c *csclient) txnGen(zone string, namespaces ...string) (kv.TxnStore, error) {
	cli, err := c.etcdClientGen(zone)
	if err != nil {
		return nil, err
	}

	return etcdkv.NewStore(
		cli.KV,
		cli.Watcher,
		c.newkvOptions(zone, namespaces...),
	)
}

func (c *csclient) heartbeatGen() sdclient.HeartbeatGen {
	return sdclient.HeartbeatGen(
		func(sid services.ServiceID) (services.HeartbeatService, error) {
			cli, err := c.etcdClientGen(sid.Zone())
			if err != nil {
				return nil, err
			}

			opts := etcdheartbeat.NewOptions().
				SetInstrumentsOptions(instrument.NewOptions().
					SetLogger(c.logger).
					SetMetricsScope(c.hbScope)).
				SetServiceID(sid)
			return etcdheartbeat.NewStore(cli, opts)
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

func (c *csclient) cacheFileFn(zone string) etcdkv.CacheFileFn {
	return etcdkv.CacheFileFn(func(namespace string) string {
		if c.opts.CacheDir() == "" {
			return ""
		}
		return filepath.Join(c.opts.CacheDir(), fileName(namespace, c.opts.Service(), zone))
	})
}

func fileName(parts ...string) string {
	// get non-empty parts
	idx := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		if i != idx {
			parts[idx] = part
		}
		idx++
	}
	parts = parts[:idx]
	s := strings.Join(parts, cacheFileSeparator)
	return strings.Replace(s, string(os.PathSeparator), cacheFileSeparator, -1) + cacheFileSuffix
}

func validateTopLevelNamespace(namespace string) error {
	if strings.HasPrefix(namespace, internalPrefix) {
		return errInvalidNamespace
	}
	return nil
}
