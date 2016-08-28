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
	"math"
	"sync"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/watch"
)

var (
	errInitTimeOut     = errors.New("timed out initializing source")
	errInvalidTopology = errors.New("could not parse latest topology update")
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
	sync.Mutex

	s      xwatch.Source
	w      xwatch.Watch
	closed bool
	logger xlog.Logger
}

func newDynamicTopology(opts DynamicOptions) (Topology, error) {
	services := opts.GetConfigServiceClient().Services()

	var (
		w   xwatch.Watch
		err error
	)
	if w, err = services.WatchInstances(opts.GetService(), opts.GetQueryOptions()); err != nil {
		return nil, err
	}

	s := xwatch.NewSource(
		newServiceTopologyInput(w, opts.GetHashGen(), opts.GetRetryOptions()),
		opts.GetInstrumentOptions().GetLogger(),
	)

	// wait on source to be ready
	if err = waitOnInit(s, opts.GetInitTimeout()); err != nil {
		return nil, err
	}

	dt := &dynamicTopology{w: w, s: s, logger: opts.GetInstrumentOptions().GetLogger()}
	return dt, nil
}

func (t *dynamicTopology) Get() Map {
	return t.s.Get().(Map)
}

func (t *dynamicTopology) GetAndSubscribe() (Map, xwatch.Watch, error) {
	m, w, err := t.s.Watch()
	return m.(Map), w, err
}

func (t *dynamicTopology) Close() {
	t.Lock()
	defer t.Unlock()

	if t.closed {
		return
	}

	t.closed = true

	t.w.Close()
	t.s.Close()
}

func waitOnInit(t xwatch.Source, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-t.WaitInit():
		return nil
	case <-time.After(d):
		return errInitTimeOut
	}
}

// serviceTopologyInput implements xwatch.SourceInput with retry
type serviceTopologyInput struct {
	w       xwatch.Watch
	retrier xretry.Retrier
	hashGen sharding.HashGen
	closed  bool
	// NB(chaowang) the watch may be initiated and not get any updates on its channel any time soon,
	// so we should try whatever value the watch has as first poll to avoid source init time out.
	// If the value the watch has is invalid, the source is still not initiated
	poll int
}

func newServiceTopologyInput(w xwatch.Watch, h sharding.HashGen, opts xretry.Options) xwatch.SourceInput {
	return &serviceTopologyInput{w: w, hashGen: h, retrier: xretry.NewRetrier(opts)}
}

func (s *serviceTopologyInput) Poll() (interface{}, error) {
	var m Map
	if finalErr := s.retrier.Attempt(func() error {
		if s.poll > 0 {
			// for normal attempts (from 2nd attempt on), block on the notification channel
			if _, ok := <-s.w.C(); !ok {
				s.closed = true
				return xwatch.ErrSourceClosed
			}
		}
		s.poll++

		var err error
		m, err = newMapFromServiceInstances(s.w.Get(), s.hashGen)
		return err
	}); finalErr != nil {
		return nil, finalErr
	}

	return m, nil
}

func newMapFromServiceInstances(data interface{}, hashGen sharding.HashGen) (Map, error) {
	si, ok := data.([]services.ServiceInstance)
	if !ok {
		return nil, errInvalidTopology
	}
	to, err := newMapOptionsFromServiceInstances(si, hashGen)
	if err != nil {
		return nil, err
	}
	if err := to.Validate(); err != nil {
		return nil, err
	}
	return newStaticMap(to), nil
}

func newMapOptionsFromServiceInstances(instances []services.ServiceInstance, hashGen sharding.HashGen) (StaticOptions, error) {
	allShards, r, err := uniqueShardsAndReplicas(instances)
	if err != nil {
		return nil, err
	}
	fn := hashGen(uint32(len(allShards)))

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
		Replicas(r).
		ShardSet(allShardSet).
		HostShardSets(hostShardSets), nil
}

func uniqueShardsAndReplicas(instances []services.ServiceInstance) ([]uint32, int, error) {
	m := make(map[uint32]int)
	for _, i := range instances {
		if i.Shards() == nil {
			return nil, 0, errInstanceHasNoShardsAssignment
		}
		for _, s := range i.Shards().Shards() {
			m[s.ID()] = m[s.ID()] + 1
		}
	}
	r := math.MaxUint32
	s := make([]uint32, 0, len(m))

	// NB(chaowang): We allow different replicas for a shard in case cluster is changing capacity
	for k, v := range m {
		s = append(s, k)
		if r > v {
			r = v
		}
	}
	return s, r, nil
}
