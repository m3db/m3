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

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement/service"
	"github.com/m3db/m3x/watch"
	"github.com/uber-go/tally"
)

const (
	defaultGaugeInterval = 10 * time.Second
)

var (
	errNotImplemented   = errors.New("not implemented")
	errNoServiceFound   = errors.New("no service found")
	errWatchInitTimeout = errors.New("service watch init time out")
	errNoServiceName    = errors.New("no service specified")
	errNoZone           = errors.New("no zone specified")
	errNoEnv            = errors.New("no env specified")
)

// NewServices returns a client of Services
func NewServices(opts Options) (services.Services, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &client{
		pManagers: map[string]*placementManager{},
		opts:      opts,
	}, nil
}

type client struct {
	sync.RWMutex

	opts      Options
	pManagers map[string]*placementManager
}

func (s *client) PlacementService(sid services.ServiceID, opts services.PlacementOptions) (services.PlacementService, error) {
	if err := validateRequest(sid); err != nil {
		return nil, err
	}

	return service.NewPlacementService(s, sid, opts), nil
}

func (s *client) Advertise(ad services.Advertisement) error {
	return errNotImplemented
}

func (s *client) Unadvertise(sid services.ServiceID, id string) error {
	return errNotImplemented
}

func (s *client) Query(sid services.ServiceID, opts services.QueryOptions) (services.Service, error) {
	if err := validateRequest(sid); err != nil {
		return nil, err
	}

	v, err := s.placement(sid)
	if err != nil {
		return nil, err
	}

	service, err := getServiceFromValue(v, sid)
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (s *client) Watch(sid services.ServiceID, opts services.QueryOptions) (xwatch.Watch, error) {
	if err := validateRequest(sid); err != nil {
		return nil, err
	}

	s.opts.InstrumentsOptions().Logger().Infof(
		"adding a watch for service: %s env: %s zone: %s includeUnhealthy: %v",
		sid.Name(),
		sid.Environment(),
		sid.Zone(),
		opts.IncludeUnhealthy(),
	)

	pMgr, err := s.getPlacementManager(sid.Zone())
	if err != nil {
		return nil, err
	}

	key := placementKey(sid.Environment(), sid.Name())

	pMgr.RLock()
	watchable, exist := pMgr.serviceWatchables[key]
	pMgr.RUnlock()
	if exist {
		_, w, err := watchable.Watch()
		return w, err
	}

	// prepare the watch of placement outside of lock
	_, valueWatch, err := initValueWatch(pMgr.kv, key, sid, s.opts.InitTimeout())
	if err != nil {
		return nil, err
	}

	initService, err := getServiceFromValue(valueWatch.Get(), sid)
	if err != nil {
		valueWatch.Close()
		return nil, err
	}

	pMgr.Lock()
	defer pMgr.Unlock()
	watchable, exist = pMgr.serviceWatchables[key]
	if exist {
		// if a watchable already exist now, we need to clean up the placement watches we just created
		valueWatch.Close()
		_, w, err := watchable.Watch()
		return w, err
	}

	watchable = xwatch.NewWatchable()
	pMgr.serviceWatchables[key] = watchable

	watchable.Update(initService)

	sdm := newServiceDiscoveryMetrics(
		s.opts.InstrumentsOptions().MetricsScope().Tagged(map[string]string{
			"sd_service": sid.Name(),
			"sd_env":     sid.Environment(),
			"sd_zone":    sid.Zone(),
		}),
	)
	go s.run(valueWatch, watchable, sid, sdm.serviceUnmalshalErr)
	go updateVersionGauge(valueWatch, sdm.versionGauge)

	_, w, err := watchable.Watch()
	return w, err
}

func (s *client) placement(sid services.ServiceID) (kv.Value, error) {
	pMgr, err := s.getPlacementManager(sid.Zone())
	if err != nil {
		return nil, err
	}

	v, err := pMgr.kv.Get(placementKey(sid.Environment(), sid.Name()))
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (s *client) getPlacementManager(zone string) (*placementManager, error) {
	s.Lock()
	defer s.Unlock()
	pManager, ok := s.pManagers[zone]
	if ok {
		return pManager, nil
	}

	kv, err := s.opts.KVGen()(zone)
	if err != nil {
		return nil, err
	}

	pManager = &placementManager{
		kv:                kv,
		serviceWatchables: map[string]xwatch.Watchable{},
	}

	s.pManagers[zone] = pManager
	return pManager, nil
}

func (s *client) run(vw kv.ValueWatch, w xwatch.Watchable, sid services.ServiceID, errCounter tally.Counter) {
	for range vw.C() {
		value := vw.Get()
		s.opts.InstrumentsOptions().Logger().Infof("received topology update notification on version %d", value.Version())

		service, err := getServiceFromValue(value, sid)
		if err != nil {
			s.opts.InstrumentsOptions().Logger().Errorf("could not unmarshal update from kv store for cluster")
			errCounter.Inc(1)
			continue
		}
		s.opts.InstrumentsOptions().Logger().Infof("successfully parsed placement on version %d", value.Version())

		w.Update(service)
	}
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

func initValueWatch(kv kv.Store, key string, sid services.ServiceID, timeoutDur time.Duration) (services.Service, kv.ValueWatch, error) {
	valueWatch, err := kv.Watch(key)
	if err != nil {
		return nil, nil, err
	}

	err = wait(valueWatch, timeoutDur)
	if err != nil {
		return nil, nil, err
	}

	return nil, valueWatch, err
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

func newServiceDiscoveryMetrics(m tally.Scope) serviceDiscoveryMetrics {
	return serviceDiscoveryMetrics{
		versionGauge:        m.Gauge("placement.verison"),
		serviceUnmalshalErr: m.Counter("placement.unmarshal.error"),
	}
}

func placementKey(env, service string) string {
	return fmt.Sprintf("[%s]%s", env, service)
}

func validateRequest(sid services.ServiceID) error {
	if sid.Environment() == "" {
		return errNoEnv
	}
	if sid.Zone() == "" {
		return errNoZone
	}
	if sid.Name() == "" {
		return errNoServiceName
	}

	return nil
}

type placementManager struct {
	sync.RWMutex
	kv                kv.Store
	serviceWatchables map[string]xwatch.Watchable
}

type serviceDiscoveryMetrics struct {
	versionGauge        tally.Gauge
	serviceUnmalshalErr tally.Counter
}
