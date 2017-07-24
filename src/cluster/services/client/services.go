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

package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	metadataproto "github.com/m3db/m3cluster/generated/proto/metadata"
	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/proto/util"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement/service"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"
	"github.com/uber-go/tally"
)

var (
	errWatchInitTimeout   = errors.New("service watch init time out")
	errNoServiceName      = errors.New("no service specified")
	errNoServiceID        = errors.New("no service id specified")
	errNoInstanceID       = errors.New("no instance id specified")
	errAdPlacementMissing = errors.New("advertisement is missing placement instance")
)

// NewServices returns a client of Services
func NewServices(opts Options) (services.Services, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &client{
		kvManagers: make(map[string]*kvManager),
		hbStores:   make(map[string]services.HeartbeatService),
		adDoneChs:  make(map[string]chan struct{}),
		ldSvcs:     make(map[leaderKey]services.LeaderService),
		opts:       opts,
		logger:     opts.InstrumentsOptions().Logger(),
		m:          opts.InstrumentsOptions().MetricsScope(),
	}, nil
}

type client struct {
	sync.RWMutex

	opts       Options
	kvManagers map[string]*kvManager
	hbStores   map[string]services.HeartbeatService
	ldSvcs     map[leaderKey]services.LeaderService
	adDoneChs  map[string]chan struct{}
	logger     xlog.Logger
	m          tally.Scope
}

func (c *client) Metadata(sid services.ServiceID) (services.Metadata, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, err
	}

	m, err := c.getKVManager(sid.Zone())
	if err != nil {
		return nil, err
	}

	v, err := m.kv.Get(metadataKey(sid))
	if err != nil {
		return nil, err
	}

	var mp metadataproto.Metadata
	if err = v.Unmarshal(&mp); err != nil {
		return nil, err
	}

	return services.NewMetadataFromProto(&mp)
}

func (c *client) SetMetadata(sid services.ServiceID, meta services.Metadata) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	m, err := c.getKVManager(sid.Zone())
	if err != nil {
		return err
	}

	mp := util.MetadataToProto(meta)
	_, err = m.kv.Set(metadataKey(sid), &mp)
	return err
}

func (c *client) PlacementService(sid services.ServiceID, opts services.PlacementOptions) (services.PlacementService, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, err
	}

	return service.NewPlacementService(c, sid, opts), nil
}

func (c *client) Advertise(ad services.Advertisement) error {
	pi := ad.PlacementInstance()
	if pi == nil {
		return errAdPlacementMissing
	}

	if err := validateAdvertisement(ad.ServiceID(), pi.ID()); err != nil {
		return err
	}

	m, err := c.Metadata(ad.ServiceID())
	if err != nil {
		return err
	}

	hb, err := c.getHeartbeatService(ad.ServiceID())
	if err != nil {
		return err
	}

	key := adKey(ad.ServiceID(), pi.ID())
	c.Lock()
	ch, ok := c.adDoneChs[key]
	if ok {
		c.Unlock()
		return fmt.Errorf("service %s, instance %s is already being advertised", ad.ServiceID(), pi.ID())
	}
	ch = make(chan struct{})
	c.adDoneChs[key] = ch
	c.Unlock()

	go func() {
		sid := ad.ServiceID()
		errCounter := c.serviceTaggedScope(sid).Counter("heartbeat.error")

		tickFn := func() {
			if isHealthy(ad) {
				if err := hb.Heartbeat(pi, m.LivenessInterval()); err != nil {
					c.logger.Errorf("could not heartbeat service %s, %v", sid.String(), err)
					errCounter.Inc(1)
				}
			}
		}

		tickFn()

		ticker := time.Tick(m.HeartbeatInterval())
		for {
			select {
			case <-ticker:
				tickFn()
			case <-ch:
				return
			}
		}
	}()
	return nil
}

func (c *client) Unadvertise(sid services.ServiceID, id string) error {
	if err := validateAdvertisement(sid, id); err != nil {
		return err
	}

	key := adKey(sid, id)

	c.Lock()
	if ch, ok := c.adDoneChs[key]; ok {
		// if this client is advertising the instance, stop it
		close(ch)
		delete(c.adDoneChs, key)
	}
	c.Unlock()

	hbStore, err := c.getHeartbeatService(sid)
	if err != nil {
		return err
	}

	return hbStore.Delete(id)
}

func (c *client) Query(sid services.ServiceID, opts services.QueryOptions) (services.Service, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, err
	}

	v, err := c.getPlacementValue(sid)
	if err != nil {
		return nil, err
	}

	service, err := getServiceFromValue(v, sid)
	if err != nil {
		return nil, err
	}

	if !opts.IncludeUnhealthy() {
		hbStore, err := c.getHeartbeatService(sid)
		if err != nil {
			return nil, err
		}

		ids, err := hbStore.Get()
		if err != nil {
			return nil, err
		}

		service = filterInstances(service, ids)
	}

	return service, nil
}

func (c *client) Watch(sid services.ServiceID, opts services.QueryOptions) (xwatch.Watch, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, err
	}

	c.logger.Infof(
		"adding a watch for service: %s env: %s zone: %s includeUnhealthy: %v",
		sid.Name(),
		sid.Environment(),
		sid.Zone(),
		opts.IncludeUnhealthy(),
	)

	kvm, err := c.getKVManager(sid.Zone())
	if err != nil {
		return nil, err
	}

	kvm.RLock()
	watchable, exist := kvm.serviceWatchables[sid.String()]
	kvm.RUnlock()
	if exist {
		_, w, err := watchable.Watch()
		return w, err
	}

	// prepare the watch of placement outside of lock
	placementWatch, err := kvm.kv.Watch(placementKey(sid))
	if err != nil {
		return nil, err
	}

	initValue, err := waitForInitValue(kvm.kv, placementWatch, sid, c.opts.InitTimeout())
	if err != nil {
		return nil, fmt.Errorf("could not get init value within timeout, err: %v", err)
	}

	initService, err := getServiceFromValue(initValue, sid)
	if err != nil {
		placementWatch.Close()
		return nil, err
	}

	kvm.Lock()
	defer kvm.Unlock()
	watchable, exist = kvm.serviceWatchables[sid.String()]
	if exist {
		// if a watchable already exist now, we need to clean up the placement watch we just created
		placementWatch.Close()
		_, w, err := watchable.Watch()
		return w, err
	}

	watchable = xwatch.NewWatchable()
	sdm := newServiceDiscoveryMetrics(c.serviceTaggedScope(sid))

	if !opts.IncludeUnhealthy() {
		hbStore, err := c.getHeartbeatService(sid)
		if err != nil {
			placementWatch.Close()
			return nil, err
		}
		heartbeatWatch, err := hbStore.Watch()
		if err != nil {
			placementWatch.Close()
			return nil, err
		}
		watchable.Update(filterInstancesWithWatch(initService, heartbeatWatch))
		go c.watchPlacementAndHeartbeat(watchable, placementWatch, heartbeatWatch, initValue, sid, initService, sdm.serviceUnmalshalErr)
	} else {
		watchable.Update(initService)
		go c.watchPlacement(watchable, placementWatch, initValue, sid, sdm.serviceUnmalshalErr)
	}

	kvm.serviceWatchables[sid.String()] = watchable

	go updateVersionGauge(placementWatch, sdm.versionGauge)

	_, w, err := watchable.Watch()
	return w, err
}

func (c *client) HeartbeatService(sid services.ServiceID) (services.HeartbeatService, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, err
	}

	return c.getHeartbeatService(sid)
}

func (c *client) getPlacementValue(sid services.ServiceID) (kv.Value, error) {
	kvm, err := c.getKVManager(sid.Zone())
	if err != nil {
		return nil, err
	}

	v, err := kvm.kv.Get(placementKey(sid))
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (c *client) getHeartbeatService(sid services.ServiceID) (services.HeartbeatService, error) {
	c.Lock()
	defer c.Unlock()
	hb, ok := c.hbStores[sid.String()]
	if ok {
		return hb, nil
	}

	hb, err := c.opts.HeartbeatGen()(sid)
	if err != nil {
		return nil, err
	}

	c.hbStores[sid.String()] = hb
	return hb, nil
}

func (c *client) LeaderService(sid services.ServiceID, opts services.ElectionOptions) (services.LeaderService, error) {
	if sid == nil {
		return nil, errNoServiceID
	}

	if opts == nil {
		opts = services.NewElectionOptions()
	}

	key := leaderCacheKey(sid, opts)

	c.RLock()
	if ld, ok := c.ldSvcs[key]; ok {
		c.RUnlock()
		return ld, nil
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	if ld, ok := c.ldSvcs[key]; ok {
		return ld, nil
	}

	ld, err := c.opts.LeaderGen()(sid, opts)
	if err != nil {
		return nil, err
	}

	c.ldSvcs[key] = ld
	return ld, nil
}

func (c *client) getKVManager(zone string) (*kvManager, error) {
	c.Lock()
	defer c.Unlock()
	m, ok := c.kvManagers[zone]
	if ok {
		return m, nil
	}

	kv, err := c.opts.KVGen()(zone)
	if err != nil {
		return nil, err
	}

	m = &kvManager{
		kv:                kv,
		serviceWatchables: map[string]xwatch.Watchable{},
	}

	c.kvManagers[zone] = m
	return m, nil
}

func (c *client) watchPlacement(
	w xwatch.Watchable,
	vw kv.ValueWatch,
	initValue kv.Value,
	sid services.ServiceID,
	errCounter tally.Counter,
) {
	for {
		select {
		case <-vw.C():
			newService := c.serviceFromUpdate(vw.Get(), initValue, sid, errCounter)
			if newService == nil {
				continue
			}

			w.Update(newService)
		}
	}
}

func (c *client) watchPlacementAndHeartbeat(
	w xwatch.Watchable,
	vw kv.ValueWatch,
	heartbeatWatch xwatch.Watch,
	initValue kv.Value,
	sid services.ServiceID,
	service services.Service,
	errCounter tally.Counter,
) {
	for {
		select {
		case <-vw.C():
			newService := c.serviceFromUpdate(vw.Get(), initValue, sid, errCounter)
			if newService == nil {
				continue
			}

			service = newService
		case <-heartbeatWatch.C():
			c.logger.Infof("received heartbeat update")
		}
		w.Update(filterInstancesWithWatch(service, heartbeatWatch))
	}
}

func (c *client) serviceFromUpdate(
	value kv.Value,
	initValue kv.Value,
	sid services.ServiceID,
	errCounter tally.Counter,
) services.Service {
	if value == nil {
		// NB(cw) this can only happen when the placement has been deleted
		// it is safer to let the user keep using the old topology
		c.logger.Info("received placement update with nil value")
		return nil
	}

	if initValue != nil && !value.IsNewer(initValue) {
		// NB(cw) this can only happen when the init wait called a Get() itself
		// so the init value did not come from the watch, when the watch gets created
		// the first update from it may from the same version.
		c.logger.Infof("received stale placement update on version %d, skip", value.Version())
		return nil
	}

	newService, err := getServiceFromValue(value, sid)
	if err != nil {
		c.logger.Errorf("could not unmarshal update from kv store for placement on version %d, %v", value.Version(), err)
		errCounter.Inc(1)
		return nil
	}

	c.logger.Infof("successfully parsed placement on version %d", value.Version())
	return newService
}

func (c *client) serviceTaggedScope(sid services.ServiceID) tally.Scope {
	return c.m.Tagged(
		map[string]string{
			"sd_service": sid.Name(),
			"sd_env":     sid.Environment(),
			"sd_zone":    sid.Zone(),
		},
	)
}

func isHealthy(ad services.Advertisement) bool {
	healthFn := ad.Health()
	return healthFn == nil || healthFn() == nil
}

func filterInstances(s services.Service, ids []string) services.Service {
	if s == nil {
		return nil
	}

	instances := make([]services.ServiceInstance, 0, len(s.Instances()))
	for _, id := range ids {
		if instance, err := s.Instance(id); err == nil {
			instances = append(instances, instance)
		}
	}

	return services.NewService().
		SetInstances(instances).
		SetSharding(s.Sharding()).
		SetReplication(s.Replication())
}

func filterInstancesWithWatch(s services.Service, hbw xwatch.Watch) services.Service {
	if hbw.Get() == nil {
		return s
	}
	return filterInstances(s, hbw.Get().([]string))
}

func updateVersionGauge(vw kv.ValueWatch, versionGauge tally.Gauge) {
	for range time.Tick(defaultGaugeInterval) {
		v := vw.Get()
		if v != nil {
			versionGauge.Update(float64(v.Version()))
		}
	}
}

func getServiceFromValue(value kv.Value, sid services.ServiceID) (services.Service, error) {
	var placement placementproto.Placement
	if err := value.Unmarshal(&placement); err != nil {
		return nil, err
	}

	return services.NewServiceFromProto(&placement, sid)
}

func waitForInitValue(kvStore kv.Store, w kv.ValueWatch, sid services.ServiceID, timeout time.Duration) (kv.Value, error) {
	if timeout <= 0 {
		timeout = defaultInitTimeout
	}
	select {
	case <-w.C():
		return w.Get(), nil
	case <-time.After(timeout):
		return kvStore.Get(placementKey(sid))
	}
}

func validateServiceID(sid services.ServiceID) error {
	if sid.Name() == "" {
		return errNoServiceName
	}

	return nil
}

func validateAdvertisement(sid services.ServiceID, id string) error {
	if sid == nil {
		return errNoServiceID
	}

	if id == "" {
		return errNoInstanceID
	}

	return nil
}

// cache key for leader service clients
type leaderKey struct {
	sid           string
	leaderTimeout time.Duration
	resignTimeout time.Duration
	ttl           int
}

func leaderCacheKey(sid services.ServiceID, opts services.ElectionOptions) leaderKey {
	return leaderKey{
		sid:           sid.String(),
		leaderTimeout: opts.LeaderTimeout(),
		resignTimeout: opts.ResignTimeout(),
		ttl:           opts.TTLSecs(),
	}
}

type kvManager struct {
	sync.RWMutex

	kv                kv.Store
	serviceWatchables map[string]xwatch.Watchable
}

func newServiceDiscoveryMetrics(m tally.Scope) serviceDiscoveryMetrics {
	return serviceDiscoveryMetrics{
		versionGauge:        m.Gauge("placement.version"),
		serviceUnmalshalErr: m.Counter("placement.unmarshal.error"),
	}
}

type serviceDiscoveryMetrics struct {
	versionGauge        tally.Gauge
	serviceUnmalshalErr tally.Counter
}
