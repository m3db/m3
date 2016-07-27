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

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/errors"
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

	// errShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	errShardIsBootstrapping = errors.New("shard is bootstrapping")

	// errShardNotBootstrapped raised when trying to flush data for a shard that's not yet bootstrapped.
	errShardNotBootstrapped = errors.New("shard is not yet bootstrapped")

	// errSeriesIsBootstrapping raised when trying to bootstrap a series that's being bootstrapped.
	errSeriesIsBootstrapping = errors.New("series is bootstrapping")

	// errSeriesNotBootstrapped raised when trying to flush data for a series that's not yet bootstrapped.
	errSeriesNotBootstrapped = errors.New("series is not yet bootstrapped")
)

// databaseBootstrapManager manages the bootstrap process.
type databaseBootstrapManager interface {
	// IsBootstrapped returns whether the database is already bootstrapped.
	IsBootstrapped() bool

	// Bootstrap performs bootstrapping for all shards owned by db. It returns an error
	// if the server is currently being bootstrapped, and nil otherwise.
	Bootstrap() error
}

type bootstrapManager struct {
	sync.RWMutex

	database    database             // storage database
	opts        m3db.DatabaseOptions // database options
	bootstrapFn m3db.NewBootstrapFn  // function to create a new bootstrap process
	state       bootstrapState       // bootstrap state
}

func newBootstrapManager(database database) databaseBootstrapManager {
	opts := database.Options()
	return &bootstrapManager{
		database:    database,
		opts:        opts,
		bootstrapFn: opts.GetBootstrapFn(),
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
	bufferFuture := bsm.opts.GetBufferFuture()
	return writeStart.Add(bufferFuture)
}

// flushTime determines the time we can flush data for after the bootstrap
// process is finished (i.e., all the shards have bootstrapped).
func (bsm *bootstrapManager) flushTime(writeStart time.Time) time.Time {
	// NB(xichen): when bootstrap process is finished, we should have bootstrapped
	// everything until writeStart + bufferFuture, which means the current time is
	// at least writeStart + bufferFuture + bufferPast.
	bufferPast := bsm.opts.GetBufferPast()
	bufferFuture := bsm.opts.GetBufferFuture()
	return writeStart.Add(bufferFuture).Add(bufferPast)
}

// NB(xichen): Bootstrap must be called after the server starts accepting writes.
func (bsm *bootstrapManager) Bootstrap() error {
	writeStart := bsm.opts.GetNowFn()()

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
	cutover := bsm.cutoverTime(writeStart)
	shards := bsm.database.getOwnedShards()
	bs := bsm.bootstrapFn()
	for _, shard := range shards {
		err := shard.Bootstrap(bs, writeStart, cutover)
		multiErr = multiErr.Add(err)
	}

	flushTime := bsm.flushTime(writeStart)
	bsm.database.flush(flushTime, false)

	bsm.Lock()
	bsm.state = bootstrapped
	bsm.Unlock()

	return multiErr.FinalError()
}
