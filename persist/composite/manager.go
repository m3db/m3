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

package composite

import (
	"sync"
	"time"

	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
)

// persistManager delegates to a set of persist managers for data persistence
type persistManager struct {
	managers []persist.Manager // persistence managers

	fns     []persist.Fn     // cached persistence functions
	closers []persist.Closer // cached persistence closers
}

// NewPersistManager creates a new composite persist manager
func NewPersistManager(managers ...persist.Manager) persist.Manager {
	return &persistManager{
		managers: managers,
		fns:      make([]persist.Fn, 0, len(managers)),
		closers:  make([]persist.Closer, 0, len(managers)),
	}
}

func (pm *persistManager) hasMultiple() bool {
	return len(pm.managers) > 1
}

func (pm *persistManager) persist(id ts.ID, segment ts.Segment) error {
	if !pm.hasMultiple() {
		var multiErr xerrors.MultiError
		for i := range pm.fns {
			err := pm.fns[i](id, segment)
			multiErr = multiErr.Add(err)
		}
		return multiErr.FinalError()
	}

	var (
		wg    sync.WaitGroup
		errCh = make(chan error, len(pm.fns))
	)

	// TODO(xichen): use a worker pool.
	for i := range pm.fns {
		pfn := pm.fns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pfn(id, segment)
			errCh <- err
		}()
	}

	wg.Wait()
	close(errCh)

	var multiErr xerrors.MultiError
	for err := range errCh {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}

func (pm *persistManager) close() {
	if !pm.hasMultiple() {
		for i := range pm.closers {
			pm.closers[i]()
		}
		return
	}

	// TODO(xichen): use a worker pool.
	var wg sync.WaitGroup
	for i := range pm.closers {
		closer := pm.closers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			closer()
		}()
	}

	wg.Wait()
}

func (pm *persistManager) createPreparedWithError(multiErr xerrors.MultiError) (persist.PreparedPersist, error) {
	var prepared persist.PreparedPersist

	// NB(xichen): if no managers need to persist this (shard, blockStart) combination,
	// we return an empty object to shortcut the persistence process without attempting
	// pm.persist on each series, which is a no-op at this point.
	if len(pm.fns) == 0 {
		return prepared, multiErr.FinalError()
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close
	return prepared, multiErr.FinalError()
}

func (pm *persistManager) Prepare(namespace ts.ID, shard uint32, blockStart time.Time) (persist.PreparedPersist, error) {
	pm.fns = pm.fns[:0]
	pm.closers = pm.closers[:0]

	// If we don't have multiple managers, we persist sequentially to avoid the cost of
	// creating goroutines.
	var multiErr xerrors.MultiError
	if !pm.hasMultiple() {
		for i := range pm.managers {
			pp, err := pm.managers[i].Prepare(namespace, shard, blockStart)
			multiErr = multiErr.Add(err)
			if pp.Persist != nil {
				pm.fns = append(pm.fns, pp.Persist)
			}
			if pp.Close != nil {
				pm.closers = append(pm.closers, pp.Close)
			}
		}

		return pm.createPreparedWithError(multiErr)
	}

	var (
		wg          sync.WaitGroup
		numManagers = len(pm.managers)
		ppCh        = make(chan persist.PreparedPersist, numManagers)
		errCh       = make(chan error, numManagers)
	)

	// TODO(xichen): use a worker pool.
	for i := range pm.managers {
		manager := pm.managers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			pp, err := manager.Prepare(namespace, shard, blockStart)
			ppCh <- pp
			errCh <- err
		}()
	}

	wg.Wait()
	close(ppCh)
	close(errCh)

	for pp := range ppCh {
		err := <-errCh
		multiErr = multiErr.Add(err)
		if pp.Persist != nil {
			pm.fns = append(pm.fns, pp.Persist)
		}
		if pp.Close != nil {
			pm.closers = append(pm.closers, pp.Close)
		}
	}

	return pm.createPreparedWithError(multiErr)
}

func (pm *persistManager) SetThroughputLimitOptions(value persist.ThroughputLimitOptions) {
	for i := range pm.managers {
		pm.managers[i].SetThroughputLimitOptions(value)
	}
}

func (pm *persistManager) ThroughputLimitOptions() persist.ThroughputLimitOptions {
	for i := range pm.managers {
		return pm.managers[i].ThroughputLimitOptions()
	}
	return nil
}
