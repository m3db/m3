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
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapping
	bootstrapped
)

var (
	// errDatabaseIsBootstrapping raised when trying to bootstrap a database that's being bootstrapped.
	errDatabaseIsBootstrapping = errors.New("database is bootstrapping")

	// errDatabaseNotBootstrapped raised when trying to query a database that's not yet bootstrapped.
	errDatabaseNotBootstrapped = errors.New("database is not yet bootstrapped")

	// errNamespaceIsBootstrapping raised when trying to bootstrap a namespace that's being bootstrapped.
	errNamespaceIsBootstrapping = errors.New("namespace is bootstrapping")

	// errNamespaceNotBootstrapped raised when trying to flush data for a namespace that's not yet bootstrapped.
	errNamespaceNotBootstrapped = errors.New("namespace is not yet bootstrapped")

	// errShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	errShardIsBootstrapping = errors.New("shard is bootstrapping")

	// errShardNotBootstrapped raised when trying to flush data for a shard that's not yet bootstrapped.
	errShardNotBootstrapped = errors.New("shard is not yet bootstrapped")
)

type bootstrapManager struct {
	sync.RWMutex

	database       database
	opts           Options
	log            xlog.Logger
	nowFn          clock.NowFn
	newBootstrapFn NewBootstrapFn
	state          bootstrapState
	fsManager      databaseFileSystemManager
}

func newBootstrapManager(
	database database,
	fsManager databaseFileSystemManager,
) databaseBootstrapManager {
	opts := database.Options()
	return &bootstrapManager{
		database:       database,
		opts:           opts,
		log:            opts.InstrumentOptions().Logger(),
		nowFn:          opts.ClockOptions().NowFn(),
		newBootstrapFn: opts.NewBootstrapFn(),
		fsManager:      fsManager,
	}
}

func (m *bootstrapManager) IsBootstrapped() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == bootstrapped
}

func (m *bootstrapManager) targetRanges(writeStart time.Time) xtime.Ranges {
	ropts := m.opts.RetentionOptions()
	start := writeStart.Add(-ropts.RetentionPeriod())
	midPoint := writeStart.
		Add(-ropts.BlockSize()).
		Add(-ropts.BufferPast()).
		Truncate(ropts.BlockSize())
	cutover := writeStart.Add(ropts.BufferFuture())

	return xtime.NewRanges().
		AddRange(xtime.Range{Start: start, End: midPoint}).
		AddRange(xtime.Range{Start: midPoint, End: cutover})
}

// NB(xichen): Bootstrap must be called after the server starts accepting writes.
func (m *bootstrapManager) Bootstrap() error {
	writeStart := m.nowFn()
	targetRanges := m.targetRanges(writeStart)

	m.Lock()
	if m.state == bootstrapped {
		m.Unlock()
		return nil
	}
	if m.state == bootstrapping {
		m.Unlock()
		return errDatabaseIsBootstrapping
	}
	m.state = bootstrapping
	m.Unlock()

	// NB(xichen): each bootstrapper should be responsible for choosing the most
	// efficient way of bootstrapping database shards, be it sequential or parallel.
	multiErr := xerrors.NewMultiError()

	bs := m.newBootstrapFn()
	for _, namespace := range m.database.getOwnedNamespaces() {
		start := m.nowFn()
		if err := namespace.Bootstrap(bs, targetRanges, writeStart); err != nil {
			multiErr = multiErr.Add(err)
		}
		end := m.nowFn()
		m.log.WithFields(
			xlog.NewLogField("namespace", namespace.ID().String()),
			xlog.NewLogField("duration", end.Sub(start).String()),
		).Info("bootstrap finished")
	}

	// At this point we have bootstrapped everything between now - retentionPeriod
	// and now, so we should run the filesystem manager to clean up files and flush
	// all the data we bootstrapped.
	rateLimitOpts := m.fsManager.RateLimitOptions()
	m.fsManager.SetRateLimitOptions(rateLimitOpts.SetLimitEnabled(false))
	m.fsManager.Run(m.nowFn(), false)
	m.fsManager.SetRateLimitOptions(rateLimitOpts)

	m.Lock()
	m.state = bootstrapped
	m.Unlock()

	return multiErr.FinalError()
}
