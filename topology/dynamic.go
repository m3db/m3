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

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"
)

var (
	errInitTimeOut               = errors.New("timed out waiting for initial value")
	errInvalidService            = errors.New("service topology is invalid")
	errUnexpectedShard           = errors.New("shard is unexpected")
	errMissingShard              = errors.New("shard is missing")
	errNotEnoughReplicasForShard = errors.New("replicas of shard is less than expected")
	errInvalidTopology           = errors.New("could not parse latest value from config service")
)

type dynamicInitializer struct {
	opts DynamicOptions
}

// NewDynamicInitializer returns a dynamic topology initializer
func NewDynamicInitializer(opts DynamicOptions) Initializer {
	return dynamicInitializer{opts}
}

func (i dynamicInitializer) Init() (Topology, error) {
	if err := i.opts.Validate(); err != nil {
		return nil, err
	}
	return newDynamicTopology(i.opts)
}

type dynamicTopology struct {
	sync.RWMutex

	watch     xwatch.Watch
	watchable xwatch.Watchable
	closed    bool
	hashGen   sharding.HashGen
	logger    xlog.Logger
}

func newDynamicTopology(opts DynamicOptions) (Topology, error) {
	services := opts.GetConfigServiceClient().Services()

	var (
		watch xwatch.Watch
		err   error
	)
	if watch, err = services.Watch(opts.GetService(), opts.GetQueryOptions()); err != nil {
		return nil, err
	}

	logger := opts.GetInstrumentOptions().GetLogger()

	if err := waitOnInit(watch, opts.GetInitTimeout()); err != nil {
		logger.Errorf("dynamic topology init timed out in %s: %v", opts.GetInitTimeout().String(), err)
		return nil, err
	}

	m, err := getMapFromUpdate(watch.Get(), opts.GetHashGen())
	if err != nil {
		logger.Errorf("dynamic topology received invalid initial value: %v", err)
		return nil, err
	}

	watchable := xwatch.NewWatchable()
	watchable.Update(m)

	dt := &dynamicTopology{watch: watch, watchable: watchable, hashGen: opts.GetHashGen(), logger: logger}
	go dt.run()
	return dt, nil
}

func (t *dynamicTopology) isClosed() bool {
	t.RLock()
	closed := t.closed
	t.RUnlock()
	return closed
}

func (t *dynamicTopology) run() {
	for !t.isClosed() {
		if _, ok := <-t.watch.C(); !ok {
			t.Close()
			break
		}

		m, err := getMapFromUpdate(t.watch.Get(), t.hashGen)
		if err != nil {
			t.logger.Warnf("dynamic topology received invalid update: %v", err)
			continue
		}
		t.watchable.Update(m)
	}
}

func (t *dynamicTopology) Get() Map {
	return t.watchable.Get().(Map)
}

func (t *dynamicTopology) Watch() (MapWatch, error) {
	_, w, err := t.watchable.Watch()
	if err != nil {
		return nil, err
	}
	return newMapWatch(w), err
}

func (t *dynamicTopology) Close() {
	t.Lock()
	defer t.Unlock()

	if t.closed {
		return
	}

	t.closed = true

	t.watch.Close()
	t.watchable.Close()
}

func waitOnInit(w xwatch.Watch, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-w.C():
		return nil
	case <-time.After(d):
		return errInitTimeOut
	}
}

func getMapFromUpdate(data interface{}, hashGen sharding.HashGen) (Map, error) {
	service, ok := data.(services.Service)
	if !ok {
		return nil, errInvalidTopology
	}
	to, err := getStaticOptions(service, hashGen)
	if err != nil {
		return nil, err
	}
	if err := to.Validate(); err != nil {
		return nil, err
	}
	return newStaticMap(to), nil
}

func getStaticOptions(service services.Service, hashGen sharding.HashGen) (StaticOptions, error) {
	if service.Replication() == nil || service.Sharding() == nil || service.Instances() == nil {
		return nil, errInvalidService
	}
	replicas := service.Replication().Replicas()
	instances := service.Instances()
	shardLen := service.Sharding().NumShards()

	allShards, err := validateInstances(instances, replicas, shardLen)
	if err != nil {
		return nil, err
	}

	fn := hashGen(shardLen)
	allShardSet, err := sharding.NewShardSet(allShards, fn)
	if err != nil {
		return nil, err
	}

	hostShardSets := make([]HostShardSet, len(instances))
	for i, instance := range instances {
		hs, err := newHostShardSetFromServiceInstance(instance, fn)
		if err != nil {
			return nil, err
		}
		hostShardSets[i] = hs
	}

	return NewStaticOptions().
		Replicas(replicas).
		ShardSet(allShardSet).
		HostShardSets(hostShardSets), nil
}

func validateInstances(instances []services.ServiceInstance, replicas, shardingLen int) ([]uint32, error) {
	m := make(map[uint32]int)
	for _, i := range instances {
		if i.Shards() == nil {
			return nil, errInstanceHasNoShardsAssignment
		}
		for _, s := range i.Shards().Shards() {
			m[s.ID()] = m[s.ID()] + 1
		}
	}
	s := make([]uint32, shardingLen)
	for i := range s {
		expectShard := uint32(i)
		count, exist := m[expectShard]
		if !exist {
			return nil, errMissingShard
		}
		if count < replicas {
			return nil, errNotEnoughReplicasForShard
		}
		delete(m, expectShard)
		s[i] = expectShard
	}

	if len(m) > 0 {
		return nil, errUnexpectedShard
	}
	return s, nil
}
