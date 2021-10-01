// Copyright (c) 2018 Uber Technologies, Inc.
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

package writer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/watch"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	acceptAllFilter = producer.FilterFunc(
		func(m producer.Message) bool {
			return true
		},
	)

	errUnknownConsumptionType = errors.New("unknown consumption type")
)

type initType int

const (
	// failOnError will fail the initialization when any error is encountered.
	failOnError initType = iota

	// allowInitValueError will not fail the initialization when the initial
	// value could not be obtained within timeout.
	// This could be used to initialize a new consumer service writer during
	// runtime so it allows the consumer service writer to continue waiting
	// for the placement update in the background.
	allowInitValueError
)

type consumerServiceWriter interface {
	// Write writes a message.
	Write(rm *producer.RefCountedMessage)

	// Init will initialize the consumer service writer.
	Init(initType) error

	// Close closes the writer and the background watch thread.
	Close()

	// SetMessageTTLNanos sets the message ttl nanoseconds.
	SetMessageTTLNanos(value int64)

	// RegisterFilter registers a filter for the consumer service.
	RegisterFilter(fn producer.FilterFunc)

	// UnregisterFilter unregisters the filter for the consumer service.
	UnregisterFilter()
}

type consumerServiceWriterMetrics struct {
	placementError    tally.Counter
	placementUpdate   tally.Counter
	filterAccepted    tally.Counter
	filterNotAccepted tally.Counter
	queueSize         tally.Gauge
}

func newConsumerServiceWriterMetrics(scope tally.Scope) consumerServiceWriterMetrics {
	return consumerServiceWriterMetrics{
		placementUpdate:   scope.Counter("placement-update"),
		placementError:    scope.Counter("placement-error"),
		filterAccepted:    scope.Counter("filter-accepted"),
		filterNotAccepted: scope.Counter("filter-not-accepted"),
		queueSize:         scope.Gauge("queue-size"),
	}
}

type consumerServiceWriterImpl struct {
	sync.Mutex

	cs           topic.ConsumerService
	ps           placement.Service
	shardWriters []shardWriter
	opts         Options
	logger       *zap.Logger

	value           watch.Value
	dataFilter      producer.FilterFunc
	router          ackRouter
	consumerWriters map[string]consumerWriter
	closed          bool
	doneCh          chan struct{}
	wg              sync.WaitGroup
	m               consumerServiceWriterMetrics
	cm              consumerWriterMetrics

	processFn watch.ProcessFn
}

func newConsumerServiceWriter(
	cs topic.ConsumerService,
	numShards uint32,
	opts Options,
) (consumerServiceWriter, error) {
	ps, err := opts.ServiceDiscovery().
		PlacementService(cs.ServiceID(), opts.PlacementOptions())
	if err != nil {
		return nil, err
	}
	ct := cs.ConsumptionType()
	if ct == topic.Unknown {
		return nil, errUnknownConsumptionType
	}
	router := newAckRouter(int(numShards))
	w := &consumerServiceWriterImpl{
		cs:              cs,
		ps:              ps,
		shardWriters:    initShardWriters(router, ct, numShards, opts),
		opts:            opts,
		logger:          opts.InstrumentOptions().Logger(),
		dataFilter:      acceptAllFilter,
		router:          router,
		consumerWriters: make(map[string]consumerWriter),
		closed:          false,
		doneCh:          make(chan struct{}),
		m:               newConsumerServiceWriterMetrics(opts.InstrumentOptions().MetricsScope()),
		cm:              newConsumerWriterMetrics(opts.InstrumentOptions().MetricsScope()),
	}
	w.processFn = w.process
	return w, nil
}

func initShardWriters(
	router ackRouter,
	ct topic.ConsumptionType,
	numberOfShards uint32,
	opts Options,
) []shardWriter {
	var (
		sws = make([]shardWriter, numberOfShards)
		m   = newMessageWriterMetrics(
			opts.InstrumentOptions().MetricsScope(),
			opts.InstrumentOptions().TimerOptions(),
			opts.WithoutConsumerScope(),
		)
		mPool messagePool
	)
	if opts.MessagePoolOptions() != nil {
		mPool = newMessagePool(opts.MessagePoolOptions())
		mPool.Init()
	}
	for i := range sws {
		switch ct {
		case topic.Shared:
			sws[i] = newSharedShardWriter(uint32(i), router, mPool, opts, m)
		case topic.Replicated:
			sws[i] = newReplicatedShardWriter(uint32(i), numberOfShards, router, mPool, opts, m)
		}
	}
	return sws
}

func (w *consumerServiceWriterImpl) Write(rm *producer.RefCountedMessage) {
	if rm.Accept(w.dataFilter) {
		w.shardWriters[rm.Shard()].Write(rm)
		w.m.filterAccepted.Inc(1)
		return
	}
	// It is not an error if the message does not pass the filter.
	w.m.filterNotAccepted.Inc(1)
}

func (w *consumerServiceWriterImpl) Init(t initType) error {
	w.wg.Add(1)
	go func() {
		w.reportMetrics()
		w.wg.Done()
	}()

	updatableFn := func() (watch.Updatable, error) {
		return w.ps.Watch()
	}
	getFn := func(updatable watch.Updatable) (interface{}, error) {
		update, err := updatable.(placement.Watch).Get()
		if err != nil {
			w.m.placementError.Inc(1)
			w.logger.Error("invalid placement update from kv", zap.Error(err))
			return nil, err
		}
		w.m.placementUpdate.Inc(1)
		return update, nil
	}
	vOptions := watch.NewOptions().
		SetInitWatchTimeout(w.opts.PlacementWatchInitTimeout()).
		SetInstrumentOptions(w.opts.InstrumentOptions()).
		SetNewUpdatableFn(updatableFn).
		SetGetUpdateFn(getFn).
		SetProcessFn(w.processFn).
		SetKey(w.opts.TopicName())
	w.value = watch.NewValue(vOptions)
	err := w.value.Watch()
	if err == nil {
		return nil
	}
	if t == allowInitValueError {
		if _, ok := err.(watch.InitValueError); ok {
			w.logger.Warn("invalid placement update, continue to watch for placement updates",
				zap.Error(err))
			return nil
		}
	}
	return fmt.Errorf("consumer service writer init error: %v", err)
}

func (w *consumerServiceWriterImpl) process(update interface{}) error {
	var (
		p         = update.(placement.Placement)
		isSharded = p.IsSharded()
	)
	// Non sharded placement is only allowed for Shared consumption type.
	if w.cs.ConsumptionType() == topic.Replicated && !isSharded {
		return fmt.Errorf("non-sharded placement for replicated consumer %s", w.cs.String())
	}
	// NB(cw): Lock can be removed as w.consumerWriters is only accessed in this thread.
	w.Lock()
	newConsumerWriters, tobeDeleted := w.diffPlacementWithLock(p)
	for i, sw := range w.shardWriters {
		if isSharded {
			sw.UpdateInstances(p.InstancesForShard(uint32(i)), newConsumerWriters)
			continue
		}
		sw.UpdateInstances(p.Instances(), newConsumerWriters)
	}
	oldConsumerWriters := w.consumerWriters
	w.consumerWriters = newConsumerWriters
	w.Unlock()
	go func() {
		for _, addr := range tobeDeleted {
			cw, ok := oldConsumerWriters[addr]
			if ok {
				cw.Close()
			}
		}
	}()
	return nil
}

func (w *consumerServiceWriterImpl) diffPlacementWithLock(newPlacement placement.Placement) (map[string]consumerWriter, []string) {
	var (
		newInstances       = newPlacement.Instances()
		newConsumerWriters = make(map[string]consumerWriter, len(newInstances))
		toBeDeleted        []string
	)
	for _, instance := range newInstances {
		id := instance.Endpoint()
		cw, ok := w.consumerWriters[id]
		if ok {
			newConsumerWriters[id] = cw
			continue
		}
		cw = newConsumerWriter(instance.Endpoint(), w.router, w.opts, w.cm)
		cw.Init()
		newConsumerWriters[id] = cw
	}

	for id := range w.consumerWriters {
		if _, ok := newConsumerWriters[id]; !ok {
			toBeDeleted = append(toBeDeleted, id)
		}
	}
	return newConsumerWriters, toBeDeleted
}

func (w *consumerServiceWriterImpl) Close() {
	w.Lock()
	if w.closed {
		w.Unlock()
		return
	}
	w.closed = true
	w.Unlock()

	w.logger.Info("closing consumer service writer", zap.String("writer", w.cs.String()))
	close(w.doneCh)
	// Blocks until all messages consuemd.
	var shardWriterWG sync.WaitGroup
	for _, sw := range w.shardWriters {
		sw := sw
		shardWriterWG.Add(1)
		go func() {
			sw.Close()
			shardWriterWG.Done()
		}()
	}
	shardWriterWG.Wait()

	w.value.Unwatch()
	for _, cw := range w.consumerWriters {
		cw.Close()
	}
	w.wg.Wait()
	w.logger.Info("closed consumer service writer", zap.String("writer", w.cs.String()))
}

func (w *consumerServiceWriterImpl) SetMessageTTLNanos(value int64) {
	for _, sw := range w.shardWriters {
		sw.SetMessageTTLNanos(value)
	}
}

func (w *consumerServiceWriterImpl) RegisterFilter(filter producer.FilterFunc) {
	w.Lock()
	w.dataFilter = filter
	w.Unlock()
}

func (w *consumerServiceWriterImpl) UnregisterFilter() {
	w.Lock()
	w.dataFilter = acceptAllFilter
	w.Unlock()
}

func (w *consumerServiceWriterImpl) reportMetrics() {
	t := time.NewTicker(w.opts.InstrumentOptions().ReportInterval())
	defer t.Stop()

	for {
		select {
		case <-w.doneCh:
			return
		case <-t.C:
			var l int
			for _, sw := range w.shardWriters {
				l += sw.QueueSize()
			}
			w.m.queueSize.Update(float64(l))
		}
	}
}
