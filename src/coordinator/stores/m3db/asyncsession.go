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
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

const (
	errNewSessionFailFmt = "M3DB session failed to initialize: %v"
)

var (
	errSessionUninitialized = errors.New("M3DB session not yet initialized")
)

// AsyncSession is a thin wrapper around an M3DB session that does not block on initialization.
// Calls to methods while uninitialized will return an uninitialized error. The done channel
// is to notify the caller that the session has finished _attempting_ to get initialized.
type AsyncSession struct {
	session client.Session
	done    chan struct{}
	err     error
}

// NewAsyncSession returns a new Session
func NewAsyncSession(c client.Client, done chan struct{}) *AsyncSession {
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

		session, err := c.NewSession()
		if err != nil {
			asyncSession.err = fmt.Errorf(errNewSessionFailFmt, err)
			return
		}

		asyncSession.session = session
		asyncSession.err = nil
	}()

	return asyncSession
}

// Write value to the database for an ID
func (s *AsyncSession) Write(namespace, id ident.ID, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	if s.err != nil {
		return s.err
	}

	return s.session.Write(namespace, id, t, value, unit, annotation)
}

// WriteTagged value to the database for an ID and given tags.
func (s *AsyncSession) WriteTagged(namespace, id ident.ID, tags ident.TagIterator, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	if s.err != nil {
		return s.err
	}

	return s.session.WriteTagged(namespace, id, tags, t, value, unit, annotation)
}

// Fetch values from the database for an ID
func (s *AsyncSession) Fetch(namespace, id ident.ID, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.session.Fetch(namespace, id, startInclusive, endExclusive)
}

// FetchIDs values from the database for a set of IDs
func (s *AsyncSession) FetchIDs(namespace ident.ID, ids ident.Iterator, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.session.FetchIDs(namespace, ids, startInclusive, endExclusive)
}

// FetchTagged resolves the provided query to known IDs, and fetches the data for them.
func (s *AsyncSession) FetchTagged(namespace ident.ID, q index.Query, opts index.QueryOptions) (results encoding.SeriesIterators, exhaustive bool, err error) {
	if s.err != nil {
		return nil, false, s.err
	}

	return s.session.FetchTagged(namespace, q, opts)
}

// FetchTaggedIDs resolves the provided query to known IDs.
func (s *AsyncSession) FetchTaggedIDs(namespace ident.ID, q index.Query, opts index.QueryOptions) (client.TaggedIDsIterator, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}

	return s.session.FetchTaggedIDs(namespace, q, opts)
}

// ShardID returns the given shard for an ID for callers
// to easily discern what shard is failing when operations
// for given IDs begin failing
func (s *AsyncSession) ShardID(id ident.ID) (uint32, error) {
	if s.err != nil {
		return 0, s.err
	}

	return s.session.ShardID(id)
}

// Close the session
func (s *AsyncSession) Close() error {
	if s.err != nil {
		return s.err
	}

	return s.session.Close()
}
