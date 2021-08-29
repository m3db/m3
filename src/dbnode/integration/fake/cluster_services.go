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

package fake

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	xwatch "github.com/m3db/m3/src/x/watch"
)

// NB(r): once a lot more feature complete move this to the m3cluster repository

// M3ClusterClient is a fake m3cluster client
type M3ClusterClient interface {
	client.Client
}

// M3ClusterServices is a fake m3cluster services
type M3ClusterServices interface {
	services.Services

	// RegisterService registers a fake m3cluster service
	RegisterService(name string, svc M3ClusterService)

	// NotifyServiceUpdate will trigger any watch to fire for a service
	NotifyServiceUpdate(name string)

	// FakePlacementService returns the fake m3cluster placement service
	FakePlacementService() M3ClusterPlacementService
}

// M3ClusterService is a fake m3cluster service, mainly used
// to add synchronization and avoid data races with the default
// service implementation included in m3cluster
type M3ClusterService interface {
	services.Service
}

// M3ClusterPlacementService is a fake m3cluster placement service
type M3ClusterPlacementService interface {
	placement.Service

	// InstanceShardsMarkedAvailable returns instance shards marked as available
	InstanceShardsMarkedAvailable() map[string][]uint32
}

// M3ClusterKVStore is a fake m3cluster kv store
type M3ClusterKVStore interface {
	kv.Store
}

// M3ClusterTxnStore is a fake m3cluster txn store
type M3ClusterTxnStore interface {
	kv.TxnStore
}

// NewM3ClusterClient creates a new fake m3cluster client
func NewM3ClusterClient(
	services M3ClusterServices,
	kvStore M3ClusterKVStore,
) M3ClusterClient {
	return &m3ClusterClient{services: services, kvStore: kvStore}
}

type m3ClusterClient struct {
	services M3ClusterServices
	kvStore  M3ClusterKVStore
	txnStore M3ClusterTxnStore
}

func (c *m3ClusterClient) Services(opts services.OverrideOptions) (services.Services, error) {
	return c.services, nil
}

func (c *m3ClusterClient) KV() (kv.Store, error) {
	return c.kvStore, nil
}

func (c *m3ClusterClient) Txn() (kv.TxnStore, error) {
	return c.txnStore, nil
}

func (c *m3ClusterClient) Store(opts kv.OverrideOptions) (kv.Store, error) {
	return c.kvStore, nil
}

func (c *m3ClusterClient) TxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	return c.txnStore, nil
}

// NewM3ClusterServices creates a new fake m3cluster services
func NewM3ClusterServices() M3ClusterServices {
	return &m3ClusterServices{
		services:         make(map[string]*m3RegisteredService),
		placementService: NewM3ClusterPlacementService(),
	}
}

// NewM3ClusterServicesWithPlacementService creates a new fake m3cluster services with given placement service.
func NewM3ClusterServicesWithPlacementService(placementSvc M3ClusterPlacementService) M3ClusterServices {
	return &m3ClusterServices{
		services:         make(map[string]*m3RegisteredService),
		placementService: placementSvc,
	}
}

type m3ClusterServices struct {
	sync.RWMutex
	services         map[string]*m3RegisteredService
	placementService M3ClusterPlacementService
}

type m3RegisteredService struct {
	service   M3ClusterService
	watchable xwatch.Watchable
}

func (s *m3ClusterServices) RegisterService(
	name string,
	svc M3ClusterService,
) {
	s.Lock()
	defer s.Unlock()
	watchable := xwatch.NewWatchable()
	watchable.Update(svc)
	s.services[name] = &m3RegisteredService{
		service:   svc,
		watchable: watchable,
	}
}

func (s *m3ClusterServices) NotifyServiceUpdate(
	name string,
) {
	s.RLock()
	defer s.RUnlock()
	svc := s.services[name].service
	s.services[name].watchable.Update(svc)
}

func (s *m3ClusterServices) FakePlacementService() M3ClusterPlacementService {
	return s.placementService
}

func (s *m3ClusterServices) Advertise(
	ad services.Advertisement,
) error {
	return fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) Unadvertise(
	service services.ServiceID,
	id string,
) error {
	return fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) Query(
	service services.ServiceID,
	opts services.QueryOptions,
) (services.Service, error) {
	s.RLock()
	defer s.RUnlock()
	if entry, ok := s.services[service.Name()]; ok {
		return entry.service, nil
	}
	return nil, fmt.Errorf("service not found: %s", service.Name())
}

func (s *m3ClusterServices) Watch(
	service services.ServiceID,
	opts services.QueryOptions,
) (services.Watch, error) {
	s.RLock()
	defer s.RUnlock()
	if entry, ok := s.services[service.Name()]; ok {
		_, watch, err := entry.watchable.Watch()
		if err != nil {
			return nil, err
		}
		return services.NewWatch(watch), nil
	}
	return nil, fmt.Errorf("service not found: %s", service.Name())
}

func (s *m3ClusterServices) Metadata(
	sid services.ServiceID,
) (services.Metadata, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) SetMetadata(
	sid services.ServiceID, m services.Metadata,
) error {
	return fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) DeleteMetadata(
	sid services.ServiceID,
) error {
	return fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) PlacementService(
	service services.ServiceID,
	popts placement.Options,
) (placement.Service, error) {
	return s.placementService, nil
}

func (s *m3ClusterServices) HeartbeatService(
	service services.ServiceID,
) (services.HeartbeatService, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *m3ClusterServices) LeaderService(
	service services.ServiceID,
	opts services.ElectionOptions,
) (services.LeaderService, error) {
	return nil, fmt.Errorf("not implemented")
}

// NewM3ClusterPlacementService creates a fake m3cluster placement service
func NewM3ClusterPlacementService() M3ClusterPlacementService {
	return &m3ClusterPlacementService{
		markedAvailable: make(map[string][]uint32),
	}
}

// NewM3ClusterPlacementServiceWithPlacement creates a fake m3cluster placement service with given placement.
func NewM3ClusterPlacementServiceWithPlacement(placement placement.Placement) M3ClusterPlacementService {
	return &m3ClusterPlacementService{
		placement:       placement,
		markedAvailable: make(map[string][]uint32),
	}
}

type m3ClusterPlacementService struct {
	placement.Service

	placement       placement.Placement
	markedAvailable map[string][]uint32
}

func (s *m3ClusterPlacementService) InstanceShardsMarkedAvailable() map[string][]uint32 {
	return s.markedAvailable
}
func (s *m3ClusterPlacementService) MarkShardsAvailable(
	instanceID string, shardIDs ...uint32,
) (placement.Placement, error) {
	s.markedAvailable[instanceID] = append(s.markedAvailable[instanceID], shardIDs...)
	return nil, nil
}

func (s *m3ClusterPlacementService) Watch() (placement.Watch, error) {
	return &m3clusterPlacementWatch{
		placement: s.placement,
	}, nil
}

type m3clusterPlacementWatch struct {
	placement.Watch

	placement placement.Placement
}

func (w *m3clusterPlacementWatch) C() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func (w *m3clusterPlacementWatch) Get() (placement.Placement, error) {
	return w.placement, nil
}

func (w *m3clusterPlacementWatch) Close() {
}

// NewM3ClusterService creates a new fake m3cluster service
func NewM3ClusterService() M3ClusterService {
	return &m3ClusterService{}
}

type m3ClusterService struct {
	sync.RWMutex
	instances   []services.ServiceInstance
	replication services.ServiceReplication
	sharding    services.ServiceSharding
}

func (s *m3ClusterService) Instance(
	instanceID string,
) (services.ServiceInstance, error) {
	s.RLock()
	defer s.RUnlock()
	for _, instance := range s.instances {
		if instance.InstanceID() == instanceID {
			return instance, nil
		}
	}
	return nil, fmt.Errorf("instance not found")
}

func (s *m3ClusterService) Instances() []services.ServiceInstance {
	s.RLock()
	defer s.RUnlock()
	return s.instances
}

func (s *m3ClusterService) Replication() services.ServiceReplication {
	s.RLock()
	defer s.RUnlock()
	return s.replication
}

func (s *m3ClusterService) Sharding() services.ServiceSharding {
	s.RLock()
	defer s.RUnlock()
	return s.sharding
}

func (s *m3ClusterService) SetInstances(
	insts []services.ServiceInstance,
) services.Service {
	s.Lock()
	defer s.Unlock()
	s.instances = insts
	return s
}

func (s *m3ClusterService) SetReplication(
	r services.ServiceReplication,
) services.Service {
	s.Lock()
	defer s.Unlock()
	s.replication = r
	return s
}

func (s *m3ClusterService) SetSharding(
	ss services.ServiceSharding,
) services.Service {
	s.Lock()
	defer s.Unlock()
	s.sharding = ss
	return s
}
