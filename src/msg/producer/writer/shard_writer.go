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

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/log"

	"go.uber.org/atomic"
)

type shardWriter interface {
	// Write writes the reference counted message, this needs to be thread safe.
	Write(rm producer.RefCountedMessage)

	// UpdateInstances updates the instances responsible for this shard.
	UpdateInstances(
		instances []placement.Instance,
		cws map[string]consumerWriter,
	)

	// Close closes the shard writer.
	Close()
}

type sharedShardWriter struct {
	instances map[string]struct{}
	mw        messageWriter
	isClosed  *atomic.Bool
}

func newSharedShardWriter(
	shard uint32,
	router ackRouter,
	mPool messagePool,
	opts Options,
) shardWriter {
	replicatedShardID := uint64(shard)
	mw := newMessageWriter(replicatedShardID, mPool, opts)
	mw.Init()
	router.Register(replicatedShardID, mw)
	return &sharedShardWriter{
		instances: make(map[string]struct{}),
		mw:        mw,
		isClosed:  atomic.NewBool(false),
	}
}

func (w *sharedShardWriter) Write(rm producer.RefCountedMessage) {
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
		w.mw.AddConsumerWriter(id, cws[id])
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

type replicatedShardWriter struct {
	sync.RWMutex

	shard          uint32
	numberOfShards uint32
	mPool          messagePool
	opts           Options
	logger         log.Logger

	ackRouter      ackRouter
	replicaID      uint32
	messageWriters map[string]messageWriter
	isClosed       bool
}

func newReplicatedShardWriter(
	shard, numberOfShards uint32,
	router ackRouter,
	mPool messagePool,
	opts Options,
) shardWriter {
	return &replicatedShardWriter{
		shard:          shard,
		numberOfShards: numberOfShards,
		mPool:          mPool,
		opts:           opts,
		logger:         opts.InstrumentOptions().Logger(),
		ackRouter:      router,
		replicaID:      0,
		messageWriters: make(map[string]messageWriter),
		isClosed:       false,
	}
}

func (w *replicatedShardWriter) Write(rm producer.RefCountedMessage) {
	w.RLock()
	mws := w.messageWriters
	w.RUnlock()
	for _, mw := range mws {
		mw.Write(rm)
	}
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
		newMessageWriters = make(map[string]messageWriter, len(instances))
		toBeClosed        []messageWriter
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
			mw.AddConsumerWriter(instance.Endpoint(), cw)
			mw.RemoveConsumerWriter(id)
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
		mw := newMessageWriter(replicatedShardID, w.mPool, w.opts)
		mw.AddConsumerWriter(instance.Endpoint(), cw)
		w.updateCutoverCutoffNanos(mw, instance)
		mw.Init()
		w.ackRouter.Register(replicatedShardID, mw)
		newMessageWriters[instance.Endpoint()] = mw
	}

	w.Lock()
	w.messageWriters = newMessageWriters
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
	mw messageWriter,
	instance placement.Instance,
) {
	s, ok := instance.Shards().Shard(w.shard)
	if !ok {
		// Unexpected.
		w.logger.Errorf("could not find shard %d on instance %s", w.shard, instance.Endpoint())
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

func anyKeyValueInMap(
	m map[placement.Instance]consumerWriter,
) (placement.Instance, consumerWriter, bool) {
	for key, value := range m {
		return key, value, true
	}
	return nil, nil, false
}
