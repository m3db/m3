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
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

type fileOpStatus int

const (
	fileOpNotStarted fileOpStatus = iota
	fileOpInProgress
	fileOpSuccess
	fileOpFailed
	fileOpDirty
)

// TODO(prateek): add tests for fileOpDirty

type fileOpState struct {
	Status      fileOpStatus
	NumFailures int
}

type fileSystemManager struct {
	databaseFlushManager
	databaseCleanupManager
	sync.RWMutex

	log      xlog.Logger
	database database
	opts     Options
	jitter   time.Duration
	status   fileOpStatus
}

func newFileSystemManager(
	database database,
	scope tally.Scope,
) (databaseFileSystemManager, error) {
	opts := database.Options()
	fileOpts := opts.FileOpOptions()
	if err := fileOpts.Validate(); err != nil {
		return nil, err
	}
	scope = scope.SubScope("fs")
	fm := newFlushManager(database, scope)
	cm := newCleanupManager(database, fm, scope)

	var jitter time.Duration
	if maxJitter := fileOpts.Jitter(); maxJitter > 0 {
		nowFn := opts.ClockOptions().NowFn()
		src := rand.NewSource(nowFn().UnixNano())
		jitter = time.Duration(float64(maxJitter) *
			(float64(src.Int63()) / float64(math.MaxInt64)))
	}

	return &fileSystemManager{
		databaseFlushManager:   fm,
		databaseCleanupManager: cm,
		log:      opts.InstrumentOptions().Logger(),
		database: database,
		opts:     opts,
		jitter:   jitter,
		status:   fileOpNotStarted,
	}, nil
}

func (m *fileSystemManager) ShouldRun(t time.Time) bool {
	// If we haven't bootstrapped yet, no actions necessary.
	if !m.database.IsBootstrapped() {
		return false
	}

	m.RLock()
	defer m.RUnlock()

	// If we are in the middle of performing file operations, bail early.
	return m.status != fileOpInProgress
}

func (m *fileSystemManager) Run(t time.Time, async bool) {
	m.Lock()
	if m.status == fileOpInProgress {
		m.Unlock()
		return
	}
	m.status = fileOpInProgress
	m.Unlock()

	// NB(xichen): perform data cleanup and flushing sequentially to minimize the impact of disk seeks.
	flushFn := func() {
		if err := m.Cleanup(t); err != nil {
			m.log.Errorf("error when cleaning up data for time %v: %v", t, err)
		}
		if err := m.Flush(t); err != nil {
			m.log.Errorf("error when flushing data for time %v: %v", t, err)
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
