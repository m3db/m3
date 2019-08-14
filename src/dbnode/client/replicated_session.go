// Copyright (c) 2019 Uber Technologies, Inc.
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

package client

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	m3sync "github.com/m3db/m3x/sync"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	maxReplicationConcurrency = 128
)

type newSessionFn func(Options) (clientSession, error)

// replicatedSession is an implementation of clientSession which replicates
// session read/writes to a set of clusters asynchronously.
type replicatedSession struct {
	session       clientSession
	asyncSessions []clientSession
	newSessionFn  newSessionFn
	workerPool    m3sync.WorkerPool
	scope         tally.Scope
	log           *zap.Logger
	metrics       replicatedSessionMetrics
}

type replicatedSessionMetrics struct {
	replicateExecuted    tally.Counter
	replicateNotExecuted tally.Counter
	replicateError       tally.Counter
}

func newReplicatedSessionMetrics(scope tally.Scope) replicatedSessionMetrics {
	return replicatedSessionMetrics{
		replicateExecuted:    scope.Counter("replicate.executed"),
		replicateNotExecuted: scope.Counter("replicate.not-executed"),
		replicateError:       scope.Counter("replicate.error"),
	}
}

// Ensure replicatedSession implements the clientSession interface.
var _ clientSession = (*replicatedSession)(nil)

type replicatedSessionOption func(*replicatedSession)

func withNewSessionFn(fn newSessionFn) replicatedSessionOption {
	return func(session *replicatedSession) {
		session.newSessionFn = fn
	}
}

func newReplicatedSession(opts MultiClusterOptions, options ...replicatedSessionOption) (clientSession, error) {
	// TODO(srobb): Replace with PooledWorkerPool once it has a GoIfAvailable method
	workerPool := m3sync.NewWorkerPool(maxReplicationConcurrency)
	workerPool.Init()

	scope := opts.InstrumentOptions().MetricsScope()

	session := replicatedSession{
		workerPool: workerPool,
		scope:      scope,
		log:        opts.InstrumentOptions().Logger(),
		metrics:    newReplicatedSessionMetrics(scope),
	}

	// Apply options
	for _, option := range options {
		option(&session)
	}

	if err := session.setSession(opts); err != nil {
		return nil, err
	}
	if err := session.setAsyncSessions(opts.OptionsForAsyncClusters()); err != nil {
		return nil, err
	}

	return &session, nil
}

type writeFunc func(Session) error

func (s replicatedSession) setSession(opts Options) error {
	session, err := s.newSessionFn(opts)
	if err != nil {
		return err
	}
	s.session = session
	return nil
}

func (s replicatedSession) setAsyncSessions(opts []Options) error {
	sessions := make([]clientSession, 0, len(opts))
	for _, oo := range opts {
		session, err := s.newSessionFn(oo)
		if err != nil {
			return err
		}
		sessions = append(sessions, session)
	}
	s.asyncSessions = sessions
	return nil
}

func (s replicatedSession) replicate(fn writeFunc) error {
	for _, asyncSession := range s.asyncSessions {
		if s.workerPool.GoIfAvailable(func() {
			if err := fn(asyncSession); err != nil {
				s.metrics.replicateError.Inc(1)
				s.log.Error("could not replicate write: %v", zap.Error(err))
			}
		}) {
			s.metrics.replicateNotExecuted.Inc(1)
		} else {
			s.metrics.replicateExecuted.Inc(1)
		}
	}
	return fn(s.session)
}

// Write value to the database for an ID
func (s replicatedSession) Write(namespace, id ident.ID, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return s.replicate(func(session Session) error {
		return session.Write(namespace, id, t, value, unit, annotation)
	})
}

// WriteTagged value to the database for an ID and given tags.
func (s replicatedSession) WriteTagged(namespace, id ident.ID, tags ident.TagIterator, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return s.replicate(func(session Session) error {
		return session.WriteTagged(namespace, id, tags, t, value, unit, annotation)
	})
}

// Fetch values from the database for an ID
func (s replicatedSession) Fetch(namespace, id ident.ID, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error) {
	return s.session.Fetch(namespace, id, startInclusive, endExclusive)
}

// FetchIDs values from the database for a set of IDs
func (s replicatedSession) FetchIDs(namespace ident.ID, ids ident.Iterator, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error) {
	return s.session.FetchIDs(namespace, ids, startInclusive, endExclusive)
}

func (s replicatedSession) Aggregate(
	ns ident.ID, q index.Query, opts index.AggregationOptions,
) (AggregatedTagsIterator, bool, error) {
	return s.session.Aggregate(ns, q, opts)
}

// FetchTagged resolves the provided query to known IDs, and fetches the data for them.
func (s replicatedSession) FetchTagged(namespace ident.ID, q index.Query, opts index.QueryOptions) (results encoding.SeriesIterators, exhaustive bool, err error) {
	return s.session.FetchTagged(namespace, q, opts)
}

// FetchTaggedIDs resolves the provided query to known IDs.
func (s replicatedSession) FetchTaggedIDs(namespace ident.ID, q index.Query, opts index.QueryOptions) (iter TaggedIDsIterator, exhaustive bool, err error) {
	return s.session.FetchTaggedIDs(namespace, q, opts)
}

// ShardID returns the given shard for an ID for callers
// to easily discern what shard is failing when operations
// for given IDs begin failing
func (s replicatedSession) ShardID(id ident.ID) (uint32, error) {
	return s.session.ShardID(id)
}

// IteratorPools exposes the internal iterator pools used by the session to clients
func (s replicatedSession) IteratorPools() (encoding.IteratorPools, error) {
	return s.session.IteratorPools()
}

// Close the session
func (s replicatedSession) Close() error {
	err := s.session.Close()
	for _, as := range s.asyncSessions {
		as.Close()
	}
	return err
}

// Origin returns the host that initiated the session.
func (s replicatedSession) Origin() topology.Host {
	return s.session.Origin()
}

// Replicas returns the replication factor.
func (s replicatedSession) Replicas() int {
	return s.session.Replicas()
}

// TopologyMap returns the current topology map. Note that the session
// has a separate topology watch than the database itself, so the two
// values can be out of sync and this method should not be relied upon
// if the current view of the topology as seen by the database is required.
func (s replicatedSession) TopologyMap() (topology.Map, error) {
	return s.session.TopologyMap()
}

// Truncate will truncate the namespace for a given shard.
func (s replicatedSession) Truncate(namespace ident.ID) (int64, error) {
	return s.session.Truncate(namespace)
}

// FetchBootstrapBlocksFromPeers will fetch the most fulfilled block
// for each series using the runtime configurable bootstrap level consistency.
func (s replicatedSession) FetchBootstrapBlocksFromPeers(
	namespace namespace.Metadata,
	shard uint32,
	start, end time.Time,
	opts result.Options,
) (result.ShardResult, error) {
	return s.session.FetchBootstrapBlocksFromPeers(namespace, shard, start, end, opts)
}

// FetchBootstrapBlocksMetadataFromPeers will fetch the blocks metadata from
// available peers using the runtime configurable bootstrap level consistency.
func (s replicatedSession) FetchBootstrapBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end time.Time,
	result result.Options,
) (PeerBlockMetadataIter, error) {
	return s.session.FetchBootstrapBlocksMetadataFromPeers(namespace, shard, start, end, result)
}

// FetchBlocksMetadataFromPeers will fetch the blocks metadata from
// available peers.
func (s replicatedSession) FetchBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end time.Time,
	consistencyLevel topology.ReadConsistencyLevel,
	result result.Options,
) (PeerBlockMetadataIter, error) {
	return s.session.FetchBlocksMetadataFromPeers(namespace, shard, start, end, consistencyLevel, result)
}

// FetchBlocksFromPeers will fetch the required blocks from the
// peers specified.
func (s replicatedSession) FetchBlocksFromPeers(
	namespace namespace.Metadata,
	shard uint32,
	consistencyLevel topology.ReadConsistencyLevel,
	metadatas []block.ReplicaMetadata,
	opts result.Options,
) (PeerBlocksIter, error) {
	return s.session.FetchBlocksFromPeers(namespace, shard, consistencyLevel, metadatas, opts)
}

func (s replicatedSession) Open() error {
	if err := s.session.Open(); err != nil {
		return err
	}
	for _, asyncSession := range s.asyncSessions {
		if err := asyncSession.Open(); err != nil {
			s.log.Error("could not open session to async cluster: %v", zap.Error(err))
		}
	}
	return nil
}
