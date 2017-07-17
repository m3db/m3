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

package dynamic

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/namespace/convert"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"
)

var (
	errInitTimeOut           = errors.New("timed out waiting for initial value")
	errRegistryAlreadyClosed = errors.New("registry already closed")
	errInvalidRegistry       = errors.New("could not parse latest value from config service")
)

type dynamicInitializer struct {
	sync.Mutex
	opts namespace.DynamicOptions
	reg  namespace.Registry
}

// NewInitializer returns a dynamic namespace initializer
func NewInitializer(opts namespace.DynamicOptions) namespace.Initializer {
	return &dynamicInitializer{opts: opts}
}

func (i *dynamicInitializer) Init() (namespace.Registry, error) {
	i.Lock()
	defer i.Unlock()

	if i.reg != nil {
		return i.reg, nil
	}

	if err := i.opts.Validate(); err != nil {
		return nil, err
	}

	reg, err := newDynamicRegistry(i.opts)
	if err != nil {
		return nil, err
	}

	i.reg = reg
	return i.reg, nil
}

type dynamicRegistry struct {
	sync.RWMutex
	opts      namespace.DynamicOptions
	initValue kv.Value
	kvWatch   kv.ValueWatch
	watchable xwatch.Watchable
	closed    bool
	logger    xlog.Logger
}

func newDynamicRegistry(opts namespace.DynamicOptions) (namespace.Registry, error) {
	kvStore, err := opts.ConfigServiceClient().KV()
	if err != nil {
		return nil, err
	}

	watch, err := kvStore.Watch(opts.NamespaceRegistryKey())
	if err != nil {
		return nil, err
	}

	logger := opts.InstrumentOptions().Logger()
	if err = waitOnInit(watch, opts.InitTimeout()); err != nil {
		logger.Errorf("dynamic namespace registry initialization timed out in %s: %v",
			opts.InitTimeout().String(), err)
		return nil, err
	}

	initValue := watch.Get()
	m, err := getMapFromUpdate(initValue)
	if err != nil {
		logger.Errorf("dynamic namespace registry received invalid initial value: %v",
			err)
		return nil, err
	}

	watchable := xwatch.NewWatchable()
	watchable.Update(m)

	dt := &dynamicRegistry{
		opts:      opts,
		initValue: initValue,
		kvWatch:   watch,
		watchable: watchable,
		logger:    logger,
	}
	go dt.run()
	return dt, nil
}

func (r *dynamicRegistry) isClosed() bool {
	r.RLock()
	closed := r.closed
	r.RUnlock()
	return closed
}

func (r *dynamicRegistry) run() {
	for !r.isClosed() {
		if _, ok := <-r.kvWatch.C(); !ok {
			r.Close()
			break
		}

		val := r.kvWatch.Get()
		if !val.IsNewer(r.initValue) {
			r.logger.Warnf("dynamic namespace registry received older version: %v, skipping",
				val.Version())
			continue
		}

		m, err := getMapFromUpdate(val)
		if err != nil {
			r.logger.Warnf("dynamic namespace registry received invalid update: %v, skipping",
				err)
			continue
		}
		r.initValue = val
		r.watchable.Update(m)
	}
}

func (r *dynamicRegistry) Watch() (namespace.Watch, error) {
	_, w, err := r.watchable.Watch()
	if err != nil {
		return nil, err
	}
	return namespace.NewWatch(w), err
}

func (r *dynamicRegistry) Close() error {
	r.Lock()
	defer r.Unlock()

	if r.closed {
		return errRegistryAlreadyClosed
	}

	r.closed = true

	r.kvWatch.Close()
	r.watchable.Close()
	return nil
}

func waitOnInit(w kv.ValueWatch, d time.Duration) error {
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

func getMapFromUpdate(data interface{}) (namespace.Map, error) {
	protoRegistry, ok := data.(nsproto.Registry)
	if !ok {
		return nil, errInvalidRegistry
	}

	var metadatas []namespace.Metadata
	for ns, opts := range protoRegistry.Namespaces {
		md, err := convert.ToMetadata(ns, opts)
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, md)
	}

	return namespace.NewMap(metadatas)
}
