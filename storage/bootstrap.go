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

	// errSeriesIsBootstrapping raised when trying to bootstrap a series that's being bootstrapped.
	errSeriesIsBootstrapping = errors.New("series is bootstrapping")

	// errSeriesNotBootstrapped raised when trying to flush data for a series that's not yet bootstrapped.
	errSeriesNotBootstrapped = errors.New("series is not yet bootstrapped")
)

type bootstrapManager struct {
	sync.RWMutex

	database       database                  // storage database
	opts           Options                   // storage options
	log            xlog.Logger               // logger
	nowFn          clock.NowFn               // now fn
	newBootstrapFn NewBootstrapFn            // function to create a new bootstrap process
	state          bootstrapState            // bootstrap state
	fsm            databaseFileSystemManager // file system manager
}

func newBootstrapManager(database database, fsm databaseFileSystemManager) databaseBootstrapManager {
	opts := database.Options()
	return &bootstrapManager{
		database:       database,
		opts:           opts,
		log:            opts.GetInstrumentOptions().GetLogger(),
		nowFn:          opts.GetClockOptions().GetNowFn(),
		newBootstrapFn: opts.GetNewBootstrapFn(),
		fsm:            fsm,
	}
}

func (bsm *bootstrapManager) IsBootstrapped() bool {
	bsm.RLock()
	state := bsm.state
	bsm.RUnlock()
	return state == bootstrapped
}

// cutoverTime is when we should cut over to the in-memory data during bootstrapping.
// Data points accumulated before cut-over time are ignored because future writes before
// server starts accepting writes are lost.
func (bsm *bootstrapManager) cutoverTime(writeStart time.Time) time.Time {
	bufferFuture := bsm.opts.GetRetentionOptions().GetBufferFuture()
	return writeStart.Add(bufferFuture)
}

// NB(xichen): Bootstrap must be called after the server starts accepting writes.
func (bsm *bootstrapManager) Bootstrap() error {
	writeStart := bsm.nowFn()
	cutover := bsm.cutoverTime(writeStart)

	bsm.Lock()
	if bsm.state == bootstrapped {
		bsm.Unlock()
		return nil
	}
	if bsm.state == bootstrapping {
		bsm.Unlock()
		return errDatabaseIsBootstrapping
	}
	bsm.state = bootstrapping
	bsm.Unlock()

	// NB(xichen): each bootstrapper should be responsible for choosing the most
	// efficient way of bootstrapping database shards, be it sequential or parallel.
	multiErr := xerrors.NewMultiError()

	bs := bsm.newBootstrapFn()
	for _, namespace := range bsm.database.getOwnedNamespaces() {
		if err := namespace.Bootstrap(bs, writeStart, cutover); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	// At this point we have bootstrapped everything between now - retentionPeriod
	// and now, so we should run the filesystem manager to clean up files and flush
	// all the data we bootstrapped.
	bsm.fsm.Run(bsm.nowFn(), false)

	bsm.Lock()
	bsm.state = bootstrapped
	bsm.Unlock()

	return multiErr.FinalError()
}
