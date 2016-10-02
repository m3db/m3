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

package storage

import (
	"math/rand"
	"sync"
	"time"
)

type fileOpStatus int

const (
	fileOpNotStarted fileOpStatus = iota
	fileOpInProgress
	fileOpSuccess
	fileOpFailed
)

type fileOpState struct {
	Status      fileOpStatus
	NumFailures int
}

type fileSystemManager struct {
	databaseFlushManager
	databaseCleanupManager
	sync.RWMutex

	database  database               // storage database
	opts      Options                // database options
	jitter    time.Duration          // file operation jitter
	status    fileOpStatus           // current file operation status
	processed map[time.Time]struct{} // times we have already processed
}

func newFileSystemManager(database database) databaseFileSystemManager {
	opts := database.Options()
	fm := newFlushManager(database)
	cm := newCleanupManager(database, fm)

	var jitter time.Duration
	if maxJitter := opts.FileOpOptions().Jitter(); maxJitter > 0 {
		nowFn := opts.ClockOptions().NowFn()
		rand.Seed(nowFn().UnixNano())
		jitter = time.Duration(rand.Int63n(int64(maxJitter)))
	}

	return &fileSystemManager{
		databaseFlushManager:   fm,
		databaseCleanupManager: cm,
		database:               database,
		opts:                   opts,
		jitter:                 jitter,
		status:                 fileOpNotStarted,
		processed:              map[time.Time]struct{}{},
	}
}

func (m *fileSystemManager) ShouldRun(t time.Time) bool {
	// If we haven't bootstrapped yet, no actions necessary.
	if !m.database.IsBootstrapped() {
		return false
	}

	m.RLock()
	defer m.RUnlock()

	// If we are in the middle of performing file operations, bail early.
	if m.status == fileOpInProgress {
		return false
	}

	// If we have processed this ID before, do nothing.
	id := m.timeID(t)
	if _, exists := m.processed[id]; exists {
		return false
	}

	return true
}

func (m *fileSystemManager) Run(t time.Time, async bool) {
	m.Lock()

	if m.status == fileOpInProgress {
		m.Unlock()
		return
	}

	id := m.timeID(t)
	if _, exists := m.processed[id]; exists {
		m.Unlock()
		return
	}

	m.status = fileOpInProgress
	m.processed[id] = struct{}{}

	m.Unlock()

	// NB(xichen): perform data cleanup and flushing sequentially to minimize the impact of disk seeks.
	flushFn := func() {
		log := m.opts.InstrumentOptions().Logger()
		if err := m.Cleanup(t); err != nil {
			log.Errorf("encountered errors when cleaning up data for time %v: %v", t, err)
		}

		if err := m.Flush(t); err != nil {
			log.Errorf("encountered errors when flushing data for time %v: %v", t, err)
		}

		m.Lock()
		m.status = fileOpNotStarted
		m.Unlock()
	}

	if !async {
		flushFn()
	} else {
		go flushFn()
	}
}

// timeID returns the id of a given time. For now we use the latest flushable
// time as the ID so we only perform flushing and cleanup once every block
// size period and can flush the data as early as possible. If we need to retry
// flushing or cleanup more frequently, can make the ID time-based (e.g., every
// 10 minutes).
func (m *fileSystemManager) timeID(t time.Time) time.Time {
	return m.FlushTimeEnd(t.Add(-m.jitter))
}
