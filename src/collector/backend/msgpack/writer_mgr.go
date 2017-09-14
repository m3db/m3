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

package msgpack

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	xerrors "github.com/m3db/m3x/errors"
)

var (
	errInstanceWriterManagerClosed = errors.New("instance writer manager closed")
)

// instanceWriterManager manages instance writers.
type instanceWriterManager interface {
	// AddInstances adds instances.
	AddInstances(instances []placement.Instance) error

	// RemoveInstances removes instancess.
	RemoveInstances(instances []placement.Instance) error

	// WriteTo writes a metric alongside its policies list to a target instance.
	WriteTo(
		instance placement.Instance,
		shardID uint32,
		mu unaggregated.MetricUnion,
		pl policy.PoliciesList,
	) error

	// Flush flushes buffered metrics.
	Flush() error

	// Close closes the writer manager.
	Close() error
}

type writerManager struct {
	sync.RWMutex

	opts    ServerOptions
	writers map[string]*refCountedWriter
	closed  bool
}

func newInstanceWriterManager(opts ServerOptions) instanceWriterManager {
	return &writerManager{
		opts:    opts,
		writers: make(map[string]*refCountedWriter),
	}
}

func (mgr *writerManager) AddInstances(instances []placement.Instance) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}
	for _, instance := range instances {
		id := instance.ID()
		writer, exists := mgr.writers[id]
		if !exists {
			writer = newRefCountedWriter(instance, mgr.opts)
			mgr.writers[id] = writer
		}
		writer.IncRef()
	}
	return nil
}

func (mgr *writerManager) RemoveInstances(instances []placement.Instance) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}
	for _, instance := range instances {
		id := instance.ID()
		writer, exists := mgr.writers[id]
		if !exists {
			continue
		}
		if writer.DecRef() == 0 {
			delete(mgr.writers, id)
		}
	}
	return nil
}

func (mgr *writerManager) WriteTo(
	instance placement.Instance,
	shard uint32,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	mgr.RLock()
	if mgr.closed {
		mgr.RUnlock()
		return errInstanceWriterManagerClosed
	}
	id := instance.ID()
	writer, exists := mgr.writers[id]
	if !exists {
		mgr.RUnlock()
		return fmt.Errorf("writer for instance %s is not found", id)
	}
	err := writer.Write(shard, mu, pl)
	mgr.RUnlock()
	return err
}

func (mgr *writerManager) Flush() error {
	mgr.RLock()
	if mgr.closed {
		mgr.RUnlock()
		return errInstanceWriterManagerClosed
	}
	multiErr := xerrors.NewMultiError()
	for _, w := range mgr.writers {
		if err := w.Flush(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	mgr.RUnlock()
	return multiErr.FinalError()
}

func (mgr *writerManager) Close() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}
	mgr.closed = true
	for _, writer := range mgr.writers {
		writer.Close()
	}
	return nil
}
