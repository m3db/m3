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

package topology

// import (
// 	"fmt"

// 	"github.com/m3db/m3cluster/client"
// 	"github.com/m3db/m3cluster/kv"
// 	"github.com/m3db/m3cluster/services"
// 	"github.com/m3db/m3x/watch"
// )

// // NB(r): once a lot more feature complete move this to the m3cluster repository

// // FakeM3ClusterClient is a fake m3cluster client
// type FakeM3ClusterClient interface {
// 	client.Client
// }

// // FakeM3ClusterServices is a fake m3cluster services
// type FakeM3ClusterServices interface {
// 	services.Services
// }

// // FakeM3ClusterKVStore is a fake m3cluster kv store
// type FakeM3ClusterKVStore interface {
// 	kv.Store
// }

// // NewM3FakeClusterClient creates a new fake m3cluster client
// func NewM3FakeClusterClient(
// 	services FakeM3ClusterServices,
// 	kvStore FakeM3ClusterKVStore,
// ) FakeM3ClusterClient {
// 	return &fakeM3ClusterClient{services: services, kvStore: kvStore}
// }

// type fakeM3ClusterClient struct {
// 	services FakeM3ClusterServices
// 	kvStore  FakeM3ClusterKVStore
// }

// func (c *fakeM3ClusterClient) Services() services.Services {
// 	return c.services
// }

// func (c *fakeM3ClusterClient) KV() kv.Store {
// 	return c.kvStore
// }

// // NewFakeM3ClusterServices creates a new fake m3cluster services
// func NewFakeM3ClusterServices() FakeM3ClusterServices {
// 	return &fakeM3ClusterServices{}
// }

// type fakeM3ClusterServices struct {
// }

// func (s *fakeM3ClusterServices) Advertise(
// 	ad services.Advertisement,
// ) error {
// 	return fmt.Errorf("not implemented")
// }

// func (s *fakeM3ClusterServices) Unadvertise(
// 	service services.ServiceID,
// 	id string,
// ) error {
// 	return fmt.Errorf("not implemented")
// }

// func (s *fakeM3ClusterServices) Query(
// 	service services.ServiceID,
// 	opts services.QueryOptions,
// ) (services.Service, error) {
// 	return nil, fmt.Errorf("not implemented")
// }

// func (s *fakeM3ClusterServices) Watch(
// 	service services.ServiceID,
// 	opts services.QueryOptions,
// ) (xwatch.Watch, error) {
// 	return nil, fmt.Errorf("not implemented")
// }

// func (s *fakeM3ClusterServices) PlacementService(
// 	service services.ServiceID,
// 	popts services.PlacementOptions,
// ) (services.PlacementService, error) {
// 	return nil, fmt.Errorf("not implemented")
// }
