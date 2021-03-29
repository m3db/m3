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

package m3db

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	errNewSessionFailFmt = "M3DB session failed to initialize: %v"
)

var (
	errSessionUninitialized = errors.New("M3DB session not yet initialized")
)

// AsyncSession is a thin wrapper around an M3DB session that does not block
// on initialization.  Calls to methods while uninitialized will return an
// uninitialized error. The done channel  is to notify the caller that the
// session has finished _attempting_ to get initialized.
type AsyncSession struct {
	sync.RWMutex
	session client.Session
	done    chan<- struct{}
	err     error
}

// NewClientFn provides a DB client.
type NewClientFn func() (client.Client, error)

// NewAsyncSession returns a new AsyncSession.
func NewAsyncSession(fn NewClientFn, done chan<- struct{}) *AsyncSession {
	asyncSession := &AsyncSession{
		done: done,
		err:  errSessionUninitialized,
	}

	go func() {
		if asyncSession.done != nil {
			defer func() {
				asyncSession.done <- struct{}{}
			}()
		}

		c, err := fn()
		if err != nil {
			asyncSession.Lock()
			asyncSession.err = fmt.Errorf(errNewSessionFailFmt, err)
			asyncSession.Unlock()
			return
		}

		session, err := c.DefaultSession()

		asyncSession.Lock()
		defer asyncSession.Unlock()
		if err != nil {
			asyncSession.err = fmt.Errorf(errNewSessionFailFmt, err)
			return
		}

		asyncSession.session = session
		asyncSession.err = nil
	}()

	return asyncSession
}

// ReadClusterAvailability returns whether the cluster is availabile for reads.
func (s *AsyncSession) ReadClusterAvailability() (bool, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return false, s.err
	}

	return s.session.ReadClusterAvailability()
}

// WriteClusterAvailability returns whether the cluster is availabile for writes.
func (s *AsyncSession) WriteClusterAvailability() (bool, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return false, s.err
	}

	return s.session.WriteClusterAvailability()
}

// Write writes a value to the database for an ID.
func (s *AsyncSession) Write(namespace, id ident.ID, t time.Time, value float64,
	unit xtime.Unit, annotation []byte) error {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return s.err
	}

	return s.session.Write(namespace, id, t, value, unit, annotation)
}

// WriteTagged writes a value to the database for an ID and given tags.
func (s *AsyncSession) WriteTagged(namespace, id ident.ID, tags ident.TagIterator,
	t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return s.err
	}

	return s.session.WriteTagged(namespace, id, tags, t, value, unit, annotation)
}

// Fetch fetches values from the database for an ID.
func (s *AsyncSession) Fetch(namespace, id ident.ID, startInclusive,
	endExclusive time.Time) (encoding.SeriesIterator, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, s.err
	}

	return s.session.Fetch(namespace, id, startInclusive, endExclusive)
}

// FetchIDs fetches values from the database for a set of IDs.
func (s *AsyncSession) FetchIDs(namespace ident.ID, ids ident.Iterator,
	startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, s.err
	}

	return s.session.FetchIDs(namespace, ids, startInclusive, endExclusive)
}

// FetchTagged resolves the provided query to known IDs, and
// fetches the data for them.
func (s *AsyncSession) FetchTagged(namespace ident.ID, q index.Query,
	opts index.QueryOptions) (encoding.SeriesIterators, client.FetchResponseMetadata, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, client.FetchResponseMetadata{}, s.err
	}

	return s.session.FetchTagged(namespace, q, opts)
}

// FetchTaggedIDs resolves the provided query to known IDs.
func (s *AsyncSession) FetchTaggedIDs(namespace ident.ID, q index.Query,
	opts index.QueryOptions) (client.TaggedIDsIterator, client.FetchResponseMetadata, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, client.FetchResponseMetadata{}, s.err
	}

	return s.session.FetchTaggedIDs(namespace, q, opts)
}

// Aggregate aggregates values from the database for the given set of constraints.
func (s *AsyncSession) Aggregate(
	namespace ident.ID,
	q index.Query,
	opts index.AggregationOptions,
) (client.AggregatedTagsIterator, client.FetchResponseMetadata, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, client.FetchResponseMetadata{}, s.err
	}

	return s.session.Aggregate(namespace, q, opts)
}

// ShardID returns the given shard for an ID for callers
// to easily discern what shard is failing when operations
// for given IDs begin failing.
func (s *AsyncSession) ShardID(id ident.ID) (uint32, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return 0, s.err
	}

	return s.session.ShardID(id)
}

// IteratorPools exposes the internal iterator pools used by the session to
// clients.
func (s *AsyncSession) IteratorPools() (encoding.IteratorPools, error) {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.session.IteratorPools()
}

// Close closes the session.
func (s *AsyncSession) Close() error {
	s.RLock()
	defer s.RUnlock()
	if s.err != nil {
		return s.err
	}

	return s.session.Close()
}
