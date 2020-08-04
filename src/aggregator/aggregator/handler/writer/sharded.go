// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/pkg/errors"
)

var (
	errShardedWriterNoWriters = errors.New("no backing writers provided")
	errShardedWriterClosed    = errors.New("sharded writer closed")
)

type shardedWriter struct {
	mutex     sync.RWMutex
	closed    bool
	writers   []*threadsafeWriter
	shardFn   sharding.AggregatedShardFn
	numShards int
}

var _ Writer = &shardedWriter{}

// NewShardedWriter shards writes to the provided writers with the given sharding fn.
func NewShardedWriter(
	writers []Writer,
	shardFn sharding.AggregatedShardFn,
	iOpts instrument.Options,
) (Writer, error) {
	if len(writers) == 0 {
		return nil, errShardedWriterNoWriters
	}

	threadsafeWriters := make([]*threadsafeWriter, 0, len(writers))
	for _, w := range writers {
		threadsafeWriters = append(threadsafeWriters, &threadsafeWriter{
			writer: w,
		})
	}

	return &shardedWriter{
		numShards: len(writers),
		writers:   threadsafeWriters,
		shardFn:   shardFn,
	}, nil
}

func (w *shardedWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	w.mutex.RLock()
	if w.closed {
		w.mutex.RUnlock()
		return errShardedWriterClosed
	}

	shardID := w.shardFn(mp.ChunkedID, w.numShards)
	writerErr := w.writers[shardID].Write(mp)
	w.mutex.RUnlock()

	return writerErr
}

func (w *shardedWriter) Flush() error {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	if w.closed {
		return errShardedWriterClosed
	}

	var multiErr xerrors.MultiError
	for i := 0; i < w.numShards; i++ {
		multiErr = multiErr.Add(w.writers[i].Flush())
	}

	if multiErr.Empty() {
		return nil
	}

	return errors.WithMessage(multiErr.FinalError(), "failed to flush sharded writer")
}

func (w *shardedWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return errShardedWriterClosed
	}
	w.closed = true

	var multiErr xerrors.MultiError
	for i := 0; i < w.numShards; i++ {
		multiErr = multiErr.Add(w.writers[i].Close())
	}

	if multiErr.Empty() {
		return nil
	}

	return errors.WithMessage(multiErr.FinalError(), "failed to close sharded writer")
}

type threadsafeWriter struct {
	mutex  sync.Mutex
	writer Writer
}

var _ Writer = &threadsafeWriter{}

func (w *threadsafeWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	w.mutex.Lock()
	err := w.writer.Write(mp)
	w.mutex.Unlock()
	return err
}

func (w *threadsafeWriter) Flush() error {
	w.mutex.Lock()
	err := w.writer.Flush()
	w.mutex.Unlock()
	return err
}

func (w *threadsafeWriter) Close() error {
	w.mutex.Lock()
	err := w.writer.Close()
	w.mutex.Unlock()
	return err
}
