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
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

type replicatedSession struct {
	session       AdminSession
	asyncSessions []AdminSession
}

// Ensure replicatedSession implements the clientSession interface.
var _ clientSession = (*replicatedSession)(nil)

func newReplicatedSession(opts MultiOptions) (*replicatedSession, error) {
	session, err := newSession(opts)
	if err != nil {
		return nil, err
	}

	// TODO(srobb): do the asyncs
	return &replicatedSession{session: session}, nil
}

// Write value to the database for an ID
func (s replicatedSession) Write(namespace, id ident.ID, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	syncErr := s.session.Write(namespace, id, t, value, unit, annotation)
	// if err := s.workerPool.DoIfAvailable(fn); err != nil {
	// emit metric
	// }
	return syncErr
}

// WriteTagged value to the database for an ID and given tags.
func (s replicatedSession) WriteTagged(namespace, id ident.ID, tags ident.TagIterator, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return nil
}

// Fetch values from the database for an ID
func (s replicatedSession) Fetch(namespace, id ident.ID, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error) {
	return s.session.Fetch(namespace, id, startInclusive, endExclusive)
}

// FetchIDs values from the database for a set of IDs
func (s replicatedSession) FetchIDs(namespace ident.ID, ids ident.Iterator, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error) {
	return s.session.FetchIDs(namespace, ids, startInclusive, endExclusive)
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
	return nil
}

// Replicas returns the replication factor.
func (s replicatedSession) Replicas() int {
	return 0
}

// TopologyMap returns the current topology map. Note that the session
// has a separate topology watch than the database itself, so the two
// values can be out of sync and this method should not be relied upon
// if the current view of the topology as seen by the database is required.
func (s replicatedSession) TopologyMap() (topology.Map, error) {
	return nil, nil
}

// Truncate will truncate the namespace for a given shard.
func (s replicatedSession) Truncate(namespace ident.ID) (int64, error) {
	return 0, nil
}

// FetchBootstrapBlocksFromPeers will fetch the most fulfilled block
// for each series using the runtime configurable bootstrap level consistency.
func (s replicatedSession) FetchBootstrapBlocksFromPeers(
	namespace namespace.Metadata,
	shard uint32,
	start, end time.Time,
	opts result.Options,
) (result.ShardResult, error) {
	return nil, nil
}

// FetchBootstrapBlocksMetadataFromPeers will fetch the blocks metadata from
// available peers using the runtime configurable bootstrap level consistency.
func (s replicatedSession) FetchBootstrapBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end time.Time,
	result result.Options,
) (PeerBlockMetadataIter, error) {
	return nil, nil
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
	return nil, nil
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
	return nil, nil
}

func (s replicatedSession) Open() error {
	return nil
}
