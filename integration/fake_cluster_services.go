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

package integration

import (
	"fmt"
	"sync"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/watch"
)

// NB(r): once a lot more feature complete move this to the m3cluster repository

// FakeM3ClusterClient is a fake m3cluster client
type FakeM3ClusterClient interface {
	client.Client
}

// FakeM3ClusterServices is a fake m3cluster services
type FakeM3ClusterServices interface {
	services.Services

	// RegisterService registers a fake m3cluster service
	RegisterService(name string, svc FakeM3ClusterService)

	// NotifyServiceUpdate will trigger any watch to fire for a service
	NotifyServiceUpdate(name string)
}

// FakeM3ClusterService is a fake m3cluster service, mainly used
// to add synchronization and avoid data races with the default
// service implementation included in m3cluster
type FakeM3ClusterService interface {
	services.Service
}

// FakeM3ClusterKVStore is a fake m3cluster kv store
type FakeM3ClusterKVStore interface {
	kv.Store
}

// NewM3FakeClusterClient creates a new fake m3cluster client
func NewM3FakeClusterClient(
	services FakeM3ClusterServices,
	kvStore FakeM3ClusterKVStore,
) FakeM3ClusterClient {
	return &fakeM3ClusterClient{services: services, kvStore: kvStore}
}

type fakeM3ClusterClient struct {
	services FakeM3ClusterServices
	kvStore  FakeM3ClusterKVStore
}

func (c *fakeM3ClusterClient) Services() services.Services {
	return c.services
}

func (c *fakeM3ClusterClient) KV() kv.Store {
	return c.kvStore
}

// NewFakeM3ClusterServices creates a new fake m3cluster services
func NewFakeM3ClusterServices() FakeM3ClusterServices {
	return &fakeM3ClusterServices{
		services: make(map[string]*fakeM3RegisteredService),
	}
}

type fakeM3ClusterServices struct {
	sync.RWMutex
	services map[string]*fakeM3RegisteredService
}

type fakeM3RegisteredService struct {
	service   FakeM3ClusterService
	watchable xwatch.Watchable
}

func (s *fakeM3ClusterServices) RegisterService(
	name string,
	svc FakeM3ClusterService,
) {
	s.Lock()
	defer s.Unlock()
	watchable := xwatch.NewWatchable()
	watchable.Update(svc)
	s.services[name] = &fakeM3RegisteredService{
		service:   svc,
		watchable: watchable,
	}
}

func (s *fakeM3ClusterServices) NotifyServiceUpdate(
	name string,
) {
	s.RLock()
	defer s.RUnlock()
	svc := s.services[name].service
	s.services[name].watchable.Update(svc)
}

func (s *fakeM3ClusterServices) Advertise(
	ad services.Advertisement,
) error {
	return fmt.Errorf("not implemented")
}

func (s *fakeM3ClusterServices) Unadvertise(
	service services.ServiceID,
	id string,
) error {
	return fmt.Errorf("not implemented")
}

func (s *fakeM3ClusterServices) Query(
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

func (s *fakeM3ClusterServices) Watch(
	service services.ServiceID,
	opts services.QueryOptions,
) (xwatch.Watch, error) {
	s.RLock()
	defer s.RUnlock()
	if entry, ok := s.services[service.Name()]; ok {
		_, watch, err := entry.watchable.Watch()
		if err != nil {
			return nil, err
		}
		return watch, nil
	}
	return nil, fmt.Errorf("service not found: %s", service.Name())
}

func (s *fakeM3ClusterServices) PlacementService(
	service services.ServiceID,
	popts services.PlacementOptions,
) (services.PlacementService, error) {
	return nil, fmt.Errorf("not implemented")
}

// NewFakeM3ClusterService creates a new fake m3cluster service
func NewFakeM3ClusterService() FakeM3ClusterService {
	return &fakeM3ClusterService{}
}

type fakeM3ClusterService struct {
	sync.RWMutex
	instances   []services.ServiceInstance
	replication services.ServiceReplication
	sharding    services.ServiceSharding
}

func (s *fakeM3ClusterService) Instance(
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

func (s *fakeM3ClusterService) Instances() []services.ServiceInstance {
	s.RLock()
	defer s.RUnlock()
	return s.instances
}

func (s *fakeM3ClusterService) Replication() services.ServiceReplication {
	s.RLock()
	defer s.RUnlock()
	return s.replication
}

func (s *fakeM3ClusterService) Sharding() services.ServiceSharding {
	s.RLock()
	defer s.RUnlock()
	return s.sharding
}

func (s *fakeM3ClusterService) SetInstances(
	insts []services.ServiceInstance,
) services.Service {
	s.Lock()
	defer s.Unlock()
	s.instances = insts
	return s
}

func (s *fakeM3ClusterService) SetReplication(
	r services.ServiceReplication,
) services.Service {
	s.Lock()
	s.replication = r
	return s
}

func (s *fakeM3ClusterService) SetSharding(
	ss services.ServiceSharding,
) services.Service {
	defer s.Unlock()
	s.sharding = ss
	return s
}
