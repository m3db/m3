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

package namespace

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errRegistryAlreadyClosed = errors.New("registry already closed")
	errInvalidRegistry       = errors.New("could not parse latest value from config service")
)

type dynamicInitializer struct {
	sync.Mutex
	opts DynamicOptions
	reg  Registry
}

// NewDynamicInitializer returns a dynamic namespace initializer
func NewDynamicInitializer(opts DynamicOptions) Initializer {
	return &dynamicInitializer{opts: opts}
}

func (i *dynamicInitializer) Init() (Registry, error) {
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
	opts         DynamicOptions
	logger       *zap.Logger
	metrics      dynamicRegistryMetrics
	watchable    xwatch.Watchable
	kvWatch      kv.ValueWatch
	currentValue kv.Value
	currentMap   Map
	closed       bool
}

type dynamicRegistryMetrics struct {
	numInvalidUpdates tally.Counter
	currentVersion    tally.Gauge
}

func newDynamicRegistryMetrics(opts DynamicOptions) dynamicRegistryMetrics {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("namespace-registry")
	return dynamicRegistryMetrics{
		numInvalidUpdates: scope.Counter("invalid-update"),
		currentVersion:    scope.Gauge("current-version"),
	}
}

func newDynamicRegistry(opts DynamicOptions) (Registry, error) {
	kvStore, err := opts.ConfigServiceClient().KV()
	if err != nil {
		return nil, err
	}

	watch, err := kvStore.Watch(opts.NamespaceRegistryKey())
	if err != nil {
		return nil, err
	}

	logger := opts.InstrumentOptions().Logger()
	var (
		watchable = xwatch.NewWatchable()
		initValue kv.Value
		dt        *dynamicRegistry
	)
	if opts.AllowEmptyInitialNamespaceRegistry() {
		initValue, err = kvStore.Get(opts.NamespaceRegistryKey())
		if err == kv.ErrNotFound {
			logger.Info("no initial namespaces found. namespaces will be added via watch updates")
			dt = &dynamicRegistry{
				opts:      opts,
				logger:    logger,
				metrics:   newDynamicRegistryMetrics(opts),
				watchable: watchable,
				kvWatch:   watch,
			}

			startRegistryWatch(dt)

			return dt, nil
		} else if err != nil {
			return nil, err
		}
	} else {
		logger.Info("waiting for dynamic namespace registry initialization, " +
			"if this takes a long time, make sure that a namespace is configured")

		<-watch.C()

		initValue = watch.Get()
	}

	logger.Info("initial namespace value received")

	m, err := getMapFromUpdate(initValue, opts.ForceColdWritesEnabled())
	if err != nil {
		logger.Error("dynamic namespace registry received invalid initial value", zap.Error(err))
		return nil, err
	}

	watchable.Update(m)

	dt = &dynamicRegistry{
		opts:         opts,
		logger:       logger,
		metrics:      newDynamicRegistryMetrics(opts),
		watchable:    watchable,
		kvWatch:      watch,
		currentValue: initValue,
		currentMap:   m,
	}

	startRegistryWatch(dt)

	return dt, nil
}

func startRegistryWatch(reg *dynamicRegistry) {
	go reg.run()
	go reg.reportMetrics()
}

func (r *dynamicRegistry) isClosed() bool {
	r.RLock()
	closed := r.closed
	r.RUnlock()
	return closed
}

func (r *dynamicRegistry) value() (kv.Value, bool) {
	r.RLock()
	defer r.RUnlock()

	if r.currentValue == nil {
		return nil, false
	}

	return r.currentValue, true
}

func (r *dynamicRegistry) maps() (Map, bool) {
	r.RLock()
	defer r.RUnlock()

	if r.currentMap == nil {
		return nil, false
	}

	return r.currentMap, true
}

func (r *dynamicRegistry) reportMetrics() {
	ticker := time.NewTicker(r.opts.InstrumentOptions().ReportInterval())
	defer ticker.Stop()

	for range ticker.C {
		if r.isClosed() {
			return
		}

		if val, ok := r.value(); ok {
			r.metrics.currentVersion.Update(float64(val.Version()))
		}
	}
}

func (r *dynamicRegistry) run() {
	for !r.isClosed() {
		if _, ok := <-r.kvWatch.C(); !ok {
			r.Close()
			break
		}

		val := r.kvWatch.Get()
		if val == nil {
			r.metrics.numInvalidUpdates.Inc(1)
			r.logger.Warn("dynamic namespace registry received nil, skipping")
			continue
		}

		currentValue, ok := r.value()
		if ok && !val.IsNewer(currentValue) {
			r.metrics.numInvalidUpdates.Inc(1)
			r.logger.Warn("dynamic namespace registry received older version, skipping",
				zap.Int("version", val.Version()))
			continue
		} else if !ok {
			r.logger.Debug("current value for dynamic registry is nil. this should only happen on initialization")
		}

		m, err := getMapFromUpdate(val, r.opts.ForceColdWritesEnabled())
		if err != nil {
			r.metrics.numInvalidUpdates.Inc(1)
			r.logger.Warn("dynamic namespace registry received invalid update, skipping",
				zap.Error(err))
			continue
		}

		currentMap, ok := r.maps()
		if ok && m.Equal(currentMap) {
			r.metrics.numInvalidUpdates.Inc(1)
			r.logger.Warn("dynamic namespace registry received identical update, skipping",
				zap.Int("version", val.Version()))
			continue
		} else if !ok {
			r.logger.Debug("current map for dynamic registry is nil. this should only happen on initialization")
		}

		r.logger.Info("dynamic namespace registry updated to version",
			zap.Int("version", val.Version()))
		r.Lock()
		r.currentValue = val
		r.currentMap = m
		r.watchable.Update(m)
		r.Unlock()
	}
}

func (r *dynamicRegistry) Watch() (Watch, error) {
	_, w, err := r.watchable.Watch()
	if err != nil {
		return nil, err
	}
	return NewWatch(w), err
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

func getMapFromUpdate(val kv.Value, forceColdWritesEnabled bool) (Map, error) {
	if val == nil {
		return nil, errInvalidRegistry
	}

	var protoRegistry nsproto.Registry
	if err := val.Unmarshal(&protoRegistry); err != nil {
		return nil, errInvalidRegistry
	}

	m, err := FromProto(protoRegistry)
	if err != nil {
		return nil, err
	}

	// NB(bodu): Force cold writes to be enabled for all ns if specified.
	if forceColdWritesEnabled {
		m, err = NewMap(ForceColdWritesEnabledForMetadatas(m.Metadatas()))
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}
