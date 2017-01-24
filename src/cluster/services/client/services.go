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
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/m3db/m3cluster/services/placement/service"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"
	"github.com/uber-go/tally"
)

var (
	errWatchInitTimeout = errors.New("service watch init time out")
	errNoServiceName    = errors.New("no service specified")
	errNoServiceID      = errors.New("no service id specified")
	errNoInstanceID     = errors.New("no instance id specified")
)

// NewServices returns a client of Services
func NewServices(opts Options) (services.Services, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &client{
		kvManagers: make(map[string]*kvManager),
		hbStores:   make(map[string]heartbeat.Store),
		adDoneChs:  make(map[string]chan struct{}),
		opts:       opts,
		logger:     opts.InstrumentsOptions().Logger(),
		m:          opts.InstrumentsOptions().MetricsScope(),
	}, nil
}

type client struct {
	sync.RWMutex

	opts       Options
	kvManagers map[string]*kvManager
	hbStores   map[string]heartbeat.Store
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

	return metadataFromProto(mp), nil
}

func (c *client) SetMetadata(sid services.ServiceID, meta services.Metadata) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	m, err := c.getKVManager(sid.Zone())
	if err != nil {
		return err
	}

	mp := metadataToProto(meta)
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
	if err := validateAdvertisement(ad.ServiceID(), ad.InstanceID()); err != nil {
		return err
	}

	m, err := c.Metadata(ad.ServiceID())
	if err != nil {
		return err
	}

	hb, err := c.getHeartbeatStore(ad.ServiceID().Zone())
	if err != nil {
		return err
	}

	key := adKey(ad.ServiceID(), ad.InstanceID())
	c.Lock()
	ch, ok := c.adDoneChs[key]
	if ok {
		c.Unlock()
		return fmt.Errorf("service %s, instance %s is already being advertised", ad.ServiceID(), ad.InstanceID())
	}
	ch = make(chan struct{})
	c.adDoneChs[key] = ch
	c.Unlock()

	go func() {
		sid := ad.ServiceID()
		errCounter := c.serviceTaggedScope(sid).Counter("heartbeat.error")

		ticker := time.Tick(m.HeartbeatInterval())
		for {
			select {
			case <-ticker:
				if isHealthy(ad) {
					if err := hb.Heartbeat(serviceKey(sid), ad.InstanceID(), m.LivenessInterval()); err != nil {
						c.logger.Errorf("could not heartbeat service %s, %v", sid.String(), err)
						errCounter.Inc(1)
					}
				}
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

	c.Lock()
	defer c.Unlock()

	key := adKey(sid, id)
	ch, ok := c.adDoneChs[key]
	if !ok {
		return fmt.Errorf("service %s, instance %s is not being advertised", sid.String(), id)
	}
	ch <- struct{}{}
	delete(c.adDoneChs, key)
	return nil
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
		hbStore, err := c.getHeartbeatStore(sid.Zone())
		if err != nil {
			return nil, err
		}

		ids, err := hbStore.Get(serviceKey(sid))
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

	key := serviceKey(sid)

	kvm.RLock()
	watchable, exist := kvm.serviceWatchables[key]
	kvm.RUnlock()
	if exist {
		_, w, err := watchable.Watch()
		return w, err
	}

	// prepare the watch of placement outside of lock
	placementWatch, err := initPlacementWatch(kvm.kv, sid, c.opts.InitTimeout())
	if err != nil {
		return nil, err
	}

	initService, err := getServiceFromValue(placementWatch.Get(), sid)
	if err != nil {
		placementWatch.Close()
		return nil, err
	}

	kvm.Lock()
	defer kvm.Unlock()
	watchable, exist = kvm.serviceWatchables[key]
	if exist {
		// if a watchable already exist now, we need to clean up the placement watch we just created
		placementWatch.Close()
		_, w, err := watchable.Watch()
		return w, err
	}

	watchable = xwatch.NewWatchable()
	sdm := newServiceDiscoveryMetrics(c.serviceTaggedScope(sid))

	if !opts.IncludeUnhealthy() {
		hbStore, err := c.getHeartbeatStore(sid.Zone())
		if err != nil {
			placementWatch.Close()
			return nil, err
		}
		heartbeatWatch, err := hbStore.Watch(serviceKey(sid))
		if err != nil {
			placementWatch.Close()
			return nil, err
		}
		watchable.Update(filterInstancesWithWatch(initService, heartbeatWatch))
		go c.watchPlacementAndHeartbeat(watchable, placementWatch, heartbeatWatch, sid, initService, sdm.serviceUnmalshalErr)
	} else {
		watchable.Update(initService)
		go c.watchPlacement(watchable, placementWatch, sid, sdm.serviceUnmalshalErr)
	}

	kvm.serviceWatchables[key] = watchable

	go updateVersionGauge(placementWatch, sdm.versionGauge)

	_, w, err := watchable.Watch()
	return w, err
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

func (c *client) getHeartbeatStore(zone string) (heartbeat.Store, error) {
	c.Lock()
	defer c.Unlock()
	hb, ok := c.hbStores[zone]
	if ok {
		return hb, nil
	}

	hb, err := c.opts.HeartbeatGen()(zone)
	if err != nil {
		return nil, err
	}

	c.hbStores[zone] = hb
	return hb, nil
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
	sid services.ServiceID,
	errCounter tally.Counter,
) {
	for {
		select {
		case <-vw.C():
			value := vw.Get()
			c.logger.Infof("received placement update notification on version %d", value.Version())

			service, err := getServiceFromValue(value, sid)
			if err != nil {
				c.logger.Errorf("could not unmarshal update from kv store for placement, %v", err)
				errCounter.Inc(1)
				continue
			}
			c.logger.Infof("successfully parsed placement on version %d", value.Version())

			w.Update(service)
		}
	}
}

func (c *client) watchPlacementAndHeartbeat(
	w xwatch.Watchable,
	vw kv.ValueWatch,
	heartbeatWatch xwatch.Watch,
	sid services.ServiceID,
	service services.Service,
	errCounter tally.Counter,
) {
	for {
		select {
		case <-vw.C():
			value := vw.Get()
			c.logger.Infof("received placement update on version %d", value.Version())

			newService, err := getServiceFromValue(value, sid)
			if err != nil {
				c.logger.Errorf("could not unmarshal update from kv store for placement, %v", err)
				errCounter.Inc(1)
				continue
			}

			c.logger.Infof("successfully parsed placement on version %d", value.Version())
			service = newService
		case <-heartbeatWatch.C():
			c.logger.Infof("received heartbeat update")
		}
		w.Update(filterInstancesWithWatch(service, heartbeatWatch))
	}
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
		versionGauge.Update(int64(vw.Get().Version()))
	}
}

func getServiceFromValue(value kv.Value, sid services.ServiceID) (services.Service, error) {
	var placement placementproto.Placement
	if err := value.Unmarshal(&placement); err != nil {
		return nil, err
	}

	return serviceFromProto(placement, sid)
}

func initPlacementWatch(kv kv.Store, sid services.ServiceID, timeoutDur time.Duration) (kv.ValueWatch, error) {
	valueWatch, err := kv.Watch(placementKey(sid))
	if err != nil {
		valueWatch.Close()
		return nil, err
	}

	err = wait(valueWatch, timeoutDur)
	if err != nil {
		valueWatch.Close()
		return nil, err
	}

	return valueWatch, nil
}

func wait(vw kv.ValueWatch, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	select {
	case <-vw.C():
		return nil
	case <-time.After(timeout):
		return errWatchInitTimeout
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
