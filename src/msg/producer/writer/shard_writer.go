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
	"sync"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/msg/producer"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type shardWriter interface {
	// Write writes the reference counted message, this needs to be thread safe.
	Write(rm *producer.RefCountedMessage)

	// UpdateInstances updates the instances responsible for this shard.
	UpdateInstances(
		instances []placement.Instance,
		cws map[string]consumerWriter,
	)

	// SetMessageTTLNanos sets the message ttl nanoseconds.
	SetMessageTTLNanos(value int64)

	// Close closes the shard writer.
	Close()

	// QueueSize returns the number of messages queued for the shard.
	QueueSize() int
}

type sharedShardWriter struct {
	instances map[string]struct{}
	mw        *messageWriter
	isClosed  atomic.Bool
}

func newSharedShardWriter(
	shard uint32,
	router ackRouter,
	mPool *messagePool,
	opts Options,
	m *messageWriterMetrics,
) shardWriter {
	replicatedShardID := uint64(shard)
	mw := newMessageWriter(replicatedShardID, mPool, opts, m)
	mw.Init()
	router.Register(replicatedShardID, mw)
	return &sharedShardWriter{
		instances: make(map[string]struct{}),
		mw:        mw,
	}
}

func (w *sharedShardWriter) Write(rm *producer.RefCountedMessage) {
	w.mw.Write(rm)
}

// This is not thread safe, must be called in one thread.
func (w *sharedShardWriter) UpdateInstances(
	instances []placement.Instance,
	cws map[string]consumerWriter,
) {
	var (
		newInstancesMap = make(map[string]struct{}, len(instances))
		toBeDeleted     = w.instances
	)
	for _, instance := range instances {
		id := instance.Endpoint()
		newInstancesMap[id] = struct{}{}
		if _, ok := toBeDeleted[id]; ok {
			// Existing instance.
			delete(toBeDeleted, id)
			continue
		}
		// Add the consumer writer to the message writer.
		w.mw.AddConsumerWriter(cws[id])
	}
	for id := range toBeDeleted {
		w.mw.RemoveConsumerWriter(id)
	}
	w.instances = newInstancesMap
}

func (w *sharedShardWriter) Close() {
	if !w.isClosed.CAS(false, true) {
		return
	}
	w.mw.Close()
}

func (w *sharedShardWriter) QueueSize() int {
	return w.mw.QueueSize()
}

func (w *sharedShardWriter) SetMessageTTLNanos(value int64) {
	w.mw.SetMessageTTLNanos(value)
}

// nolint: maligned
type replicatedShardWriter struct {
	sync.RWMutex

	shard          uint32
	numberOfShards uint32
	mPool          *messagePool
	ackRouter      ackRouter
	opts           Options
	logger         *zap.Logger
	m              *messageWriterMetrics

	messageWriters  map[string]*messageWriter
	messageTTLNanos int64
	replicaID       uint32
	isClosed        bool
}

func newReplicatedShardWriter(
	shard, numberOfShards uint32,
	router ackRouter,
	mPool *messagePool,
	opts Options,
	m *messageWriterMetrics,
) shardWriter {
	return &replicatedShardWriter{
		shard:          shard,
		numberOfShards: numberOfShards,
		mPool:          mPool,
		opts:           opts,
		logger:         opts.InstrumentOptions().Logger(),
		ackRouter:      router,
		replicaID:      0,
		messageWriters: make(map[string]*messageWriter),
		isClosed:       false,
		m:              m,
	}
}

func (w *replicatedShardWriter) Write(rm *producer.RefCountedMessage) {
	w.RLock()
	if len(w.messageWriters) == 0 {
		w.RUnlock()
		w.m.noWritersError.Inc(1)
		w.logger.Error("no message writers available for shard", zap.Uint32("shard", rm.Shard()))
		return
	}
	for _, mw := range w.messageWriters {
		mw.Write(rm)
	}
	w.RUnlock()
}

// This is not thread safe, must be called in one thread.
func (w *replicatedShardWriter) UpdateInstances(
	instances []placement.Instance,
	cws map[string]consumerWriter,
) {
	// TODO: Schedule time after shardcutoff to clean up message writers that
	// are already cutoff. Otherwise it will wait until next placement change
	// to clean up.
	var (
		newMessageWriters = make(map[string]*messageWriter, len(instances))
		toBeClosed        []*messageWriter
		toBeAdded         = make(map[placement.Instance]consumerWriter, len(instances))
		oldMessageWriters = w.messageWriters
	)
	for _, instance := range instances {
		key := instance.Endpoint()
		if mw, ok := oldMessageWriters[key]; ok {
			newMessageWriters[key] = mw
			// Existing instance, try to update cutover cutoff times.
			w.updateCutoverCutoffNanos(mw, instance)
			continue
		}
		// This is a new instance.
		toBeAdded[instance] = cws[key]
	}
	for id, mw := range oldMessageWriters {
		if _, ok := newMessageWriters[id]; ok {
			// Still in the new placement.
			continue
		}
		// Keep the existing message writer and swap the consumer writer in it
		// with  a new consumer writer in the placement update, so that the
		// messages buffered in the existing message writer can be tried on
		// the new consumer writer.
		if instance, cw, ok := anyKeyValueInMap(toBeAdded); ok {
			mw.AddConsumerWriter(cw)
			mw.RemoveConsumerWriter(id)
			// a replicated writer only has a single downstream consumer instance at a time so we can update the
			// metrics with a useful consumer label.
			mw.SetMetrics(mw.Metrics().withConsumer(instance.ID()))
			w.updateCutoverCutoffNanos(mw, instance)
			newMessageWriters[instance.Endpoint()] = mw
			delete(toBeAdded, instance)
			continue
		}
		toBeClosed = append(toBeClosed, mw)
	}

	// If there are more instances for this shard, this happens when user
	// increased replication factor for the placement or just this shard.
	for instance, cw := range toBeAdded {
		replicatedShardID := uint64(w.replicaID*w.numberOfShards + w.shard)
		w.replicaID++
		mw := newMessageWriter(replicatedShardID, w.mPool, w.opts, w.m)
		mw.AddConsumerWriter(cw)
		mw.SetMetrics(mw.Metrics().withConsumer(instance.ID()))
		w.updateCutoverCutoffNanos(mw, instance)
		mw.Init()
		w.ackRouter.Register(replicatedShardID, mw)
		newMessageWriters[instance.Endpoint()] = mw
	}

	w.Lock()
	w.messageWriters = newMessageWriters
	w.setMessageTTLNanosWithLock(w.messageTTLNanos)
	w.Unlock()

	// If there are less instances for this shard, this happens when user
	// reduced replication factor for the placement or just this shard.
	for _, mw := range toBeClosed {
		mw := mw
		// This needs to be in done in a go routine as closing a message writer will
		// block until all messages consumed.
		go func() {
			mw.Close()
			w.ackRouter.Unregister(mw.ReplicatedShardID())
		}()
	}
}

func (w *replicatedShardWriter) updateCutoverCutoffNanos(
	mw *messageWriter,
	instance placement.Instance,
) {
	s, ok := instance.Shards().Shard(w.shard)
	if !ok {
		// Unexpected.
		w.logger.Error("could not find shard on instance",
			zap.Uint32("shard", w.shard), zap.String("instance", instance.Endpoint()))
		return
	}
	mw.SetCutoffNanos(s.CutoffNanos())
	mw.SetCutoverNanos(s.CutoverNanos())
}

func (w *replicatedShardWriter) Close() {
	w.Lock()
	defer w.Unlock()

	if w.isClosed {
		return
	}
	w.isClosed = true
	for _, mw := range w.messageWriters {
		mw.Close()
	}
}

func (w *replicatedShardWriter) QueueSize() int {
	w.RLock()
	mws := w.messageWriters
	var l int
	for _, mw := range mws {
		l += mw.QueueSize()
	}
	w.RUnlock()
	return l
}

func (w *replicatedShardWriter) SetMessageTTLNanos(value int64) {
	w.Lock()
	w.messageTTLNanos = value
	w.setMessageTTLNanosWithLock(value)
	w.Unlock()
}

func (w *replicatedShardWriter) setMessageTTLNanosWithLock(value int64) {
	for _, mw := range w.messageWriters {
		mw.SetMessageTTLNanos(value)
	}
}

func anyKeyValueInMap(
	m map[placement.Instance]consumerWriter,
) (placement.Instance, consumerWriter, bool) {
	for key, value := range m {
		return key, value, true
	}
	return nil, nil, false
}
