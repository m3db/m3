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
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	etcdkv "github.com/m3db/m3/src/cluster/kv/etcd"
	"github.com/m3db/m3/src/cluster/services"
	etcdheartbeat "github.com/m3db/m3/src/cluster/services/heartbeat/etcd"
	"github.com/m3db/m3/src/cluster/services/leader"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"

	"github.com/uber-go/tally"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	hierarchySeparator = "/"
	internalPrefix     = "_"
	cacheFileSeparator = "_"
	cacheFileSuffix    = ".json"
	// TODO deprecate this once all keys are migrated to per service namespace
	kvPrefix = "_kv"
)

var errInvalidNamespace = errors.New("invalid namespace")

// make sure m3cluster and etcd client interfaces are implemented, and that
// Client is a superset of cluster.Client.
var _ client.Client = Client((*csclient)(nil))

type newClientFn func(cluster Cluster) (*clientv3.Client, error)

type cacheFileForZoneFn func(zone string) etcdkv.CacheFileFn

// ZoneClient is a cached etcd client for a zone.
type ZoneClient struct {
	Client *clientv3.Client
	Zone   string
}

// NewEtcdConfigServiceClient returns a new etcd-backed cluster client.
//nolint:golint
func NewEtcdConfigServiceClient(opts Options) (*csclient, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	scope := opts.InstrumentOptions().
		MetricsScope().
		Tagged(map[string]string{"service": opts.Service()})

	return &csclient{
		opts:    opts,
		sdOpts:  opts.ServicesOptions(),
		kvScope: scope.Tagged(map[string]string{"config_service": "kv"}),
		sdScope: scope.Tagged(map[string]string{"config_service": "sd"}),
		hbScope: scope.Tagged(map[string]string{"config_service": "hb"}),
		clis:    make(map[string]*clientv3.Client),
		logger:  opts.InstrumentOptions().Logger(),
		newFn:   newClient,
		retrier: retry.NewRetrier(opts.RetryOptions()),
		stores:  make(map[string]kv.TxnStore),
	}, nil
}

// NewConfigServiceClient returns a ConfigServiceClient.
func NewConfigServiceClient(opts Options) (client.Client, error) {
	return NewEtcdConfigServiceClient(opts)
}

type csclient struct {
	sync.RWMutex
	clis map[string]*clientv3.Client

	opts    Options
	sdOpts  services.Options
	kvScope tally.Scope
	sdScope tally.Scope
	hbScope tally.Scope
	logger  *zap.Logger
	newFn   newClientFn
	retrier retry.Retrier

	storeLock sync.Mutex
	stores    map[string]kv.TxnStore
}

func (c *csclient) Services(opts services.OverrideOptions) (services.Services, error) {
	if opts == nil {
		opts = services.NewOverrideOptions()
	}
	return c.createServices(opts)
}

func (c *csclient) KV() (kv.Store, error) {
	return c.Txn()
}

func (c *csclient) Txn() (kv.TxnStore, error) {
	return c.TxnStore(kv.NewOverrideOptions())
}

func (c *csclient) Store(opts kv.OverrideOptions) (kv.Store, error) {
	return c.TxnStore(opts)
}

func (c *csclient) TxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	opts, err := c.sanitizeOptions(opts)
	if err != nil {
		return nil, err
	}

	return c.createTxnStore(opts)
}

func (c *csclient) createServices(opts services.OverrideOptions) (services.Services, error) {
	nOpts := opts.NamespaceOptions()
	cacheFileExtraFields := []string{nOpts.PlacementNamespace(), nOpts.MetadataNamespace()}
	return services.NewServices(c.sdOpts.
		SetHeartbeatGen(c.heartbeatGen()).
		SetKVGen(c.kvGen(c.cacheFileFn(cacheFileExtraFields...))).
		SetLeaderGen(c.leaderGen()).
		SetNamespaceOptions(nOpts).
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.sdScope),
		),
	)
}

func (c *csclient) createTxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	// validate the override options because they are user supplied.
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return c.txnGen(opts, c.cacheFileFn())
}

func (c *csclient) kvGen(fn cacheFileForZoneFn) services.KVGen {
	return services.KVGen(func(zone string) (kv.Store, error) {
		// we don't validate or sanitize the options here because we're using
		// them as a container for zone.
		opts := kv.NewOverrideOptions().SetZone(zone)
		return c.txnGen(opts, fn)
	})
}

func (c *csclient) newkvOptions(
	opts kv.OverrideOptions,
	cacheFileFn cacheFileForZoneFn,
) etcdkv.Options {
	kvOpts := etcdkv.NewOptions().
		SetInstrumentsOptions(c.opts.InstrumentOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.kvScope)).
		SetCacheFileFn(cacheFileFn(opts.Zone())).
		SetWatchWithRevision(c.opts.WatchWithRevision()).
		SetNewDirectoryMode(c.opts.NewDirectoryMode()).
		SetEnableFastGets(c.opts.EnableFastGets()).
		SetRetryOptions(c.opts.RetryOptions()).
		SetRequestTimeout(c.opts.RequestTimeout()).
		SetWatchChanInitTimeout(c.opts.WatchChanInitTimeout()).
		SetWatchChanCheckInterval(c.opts.WatchChanCheckInterval()).
		SetWatchChanResetInterval(c.opts.WatchChanResetInterval())

	if ns := opts.Namespace(); ns != "" {
		kvOpts = kvOpts.SetPrefix(kvOpts.ApplyPrefix(ns))
	}

	if env := opts.Environment(); env != "" {
		kvOpts = kvOpts.SetPrefix(kvOpts.ApplyPrefix(env))
	}

	return kvOpts
}

// txnGen assumes the caller has validated the options passed if they are
// user-supplied (as opposed to constructed ourselves).
func (c *csclient) txnGen(
	opts kv.OverrideOptions,
	cacheFileFn cacheFileForZoneFn,
) (kv.TxnStore, error) {
	cli, err := c.etcdClientGen(opts.Zone())
	if err != nil {
		return nil, err
	}

	c.storeLock.Lock()
	defer c.storeLock.Unlock()

	key := kvStoreCacheKey(opts.Zone(), opts.Namespace(), opts.Environment())
	store, ok := c.stores[key]
	if ok {
		return store, nil
	}
	if store, err = etcdkv.NewStore(cli, c.newkvOptions(opts, cacheFileFn)); err != nil {
		return nil, err
	}

	c.stores[key] = store
	return store, nil
}

func (c *csclient) heartbeatGen() services.HeartbeatGen {
	return services.HeartbeatGen(
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

func (c *csclient) leaderGen() services.LeaderGen {
	return services.LeaderGen(
		func(sid services.ServiceID, eo services.ElectionOptions) (services.LeaderService, error) {
			cli, err := c.etcdClientGen(sid.Zone())
			if err != nil {
				return nil, err
			}

			opts := leader.NewOptions().
				SetServiceID(sid).
				SetElectionOpts(eo)

			return leader.NewService(cli, opts)
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
		return nil, fmt.Errorf("no etcd cluster found for zone: %s", zone)
	}

	err := c.retrier.Attempt(func() error {
		var tryErr error
		cli, tryErr = c.newFn(cluster)
		return tryErr
	})
	if err != nil {
		return nil, err
	}

	c.clis[zone] = cli
	return cli, nil
}

// Clients returns all currently cached etcd clients.
func (c *csclient) Clients() []ZoneClient {
	c.Lock()
	defer c.Unlock()

	var (
		zones   = make([]string, 0, len(c.clis))
		clients = make([]ZoneClient, 0, len(c.clis))
	)

	for k := range c.clis {
		zones = append(zones, k)
	}

	sort.Strings(zones)

	for _, zone := range zones {
		clients = append(clients, ZoneClient{Zone: zone, Client: c.clis[zone]})
	}

	return clients
}

func newClient(cluster Cluster) (*clientv3.Client, error) {
	tls, err := cluster.TLSOptions().Config()
	if err != nil {
		return nil, err
	}
	cfg := clientv3.Config{
		AutoSyncInterval: cluster.AutoSyncInterval(),
		DialTimeout:      cluster.DialTimeout(),
		Endpoints:        cluster.Endpoints(),
		TLS:              tls,
	}

	if opts := cluster.KeepAliveOptions(); opts.KeepAliveEnabled() {
		keepAlivePeriod := opts.KeepAlivePeriod()
		if maxJitter := opts.KeepAlivePeriodMaxJitter(); maxJitter > 0 {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			jitter := rnd.Int63n(int64(maxJitter))
			keepAlivePeriod += time.Duration(jitter)
		}
		cfg.DialKeepAliveTime = keepAlivePeriod
		cfg.DialKeepAliveTimeout = opts.KeepAliveTimeout()
		cfg.PermitWithoutStream = true
	}

	return clientv3.New(cfg)
}

func (c *csclient) cacheFileFn(extraFields ...string) cacheFileForZoneFn {
	return func(zone string) etcdkv.CacheFileFn {
		return func(namespace string) string {
			if c.opts.CacheDir() == "" {
				return ""
			}

			cacheFileFields := make([]string, 0, len(extraFields)+3)
			cacheFileFields = append(cacheFileFields, namespace, c.opts.Service(), zone)
			cacheFileFields = append(cacheFileFields, extraFields...)
			return filepath.Join(c.opts.CacheDir(), fileName(cacheFileFields...))
		}
	}
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
	if namespace == "" || namespace == hierarchySeparator {
		return errInvalidNamespace
	}
	if strings.HasPrefix(namespace, internalPrefix) {
		// start with _
		return errInvalidNamespace
	}
	if strings.HasPrefix(namespace, hierarchySeparator+internalPrefix) {
		return errInvalidNamespace
	}
	return nil
}

func (c *csclient) sanitizeOptions(opts kv.OverrideOptions) (kv.OverrideOptions, error) {
	if opts.Zone() == "" {
		opts = opts.SetZone(c.opts.Zone())
	}

	if opts.Environment() == "" {
		opts = opts.SetEnvironment(c.opts.Env())
	}

	namespace := opts.Namespace()
	if namespace == "" {
		return opts.SetNamespace(kvPrefix), nil
	}

	if err := validateTopLevelNamespace(namespace); err != nil {
		return nil, err
	}

	return opts, nil
}

func kvStoreCacheKey(zone string, namespaces ...string) string {
	parts := make([]string, 0, 1+len(namespaces))
	parts = append(parts, zone)
	for _, ns := range namespaces {
		if ns != "" {
			parts = append(parts, ns)
		}
	}
	return strings.Join(parts, hierarchySeparator)
}
