package client

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// NewNoopSession returns a noop session.
func NewNoopSession() Session {
	return noopSession{}
}

type noopSession struct{}

// Write value to the database for an ID.
func (noopSession) Write(namespace ident.ID, id ident.ID, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return nil
}

// WriteTagged value to the database for an ID and given tags.
func (noopSession) WriteTagged(namespace ident.ID, id ident.ID, tags ident.TagIterator, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return nil
}

// Fetch values from the database for an ID.
func (noopSession) Fetch(namespace ident.ID, id ident.ID, startInclusive time.Time, endExclusive time.Time) (encoding.SeriesIterator, error) {
	return encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{}, nil), nil
}

// FetchIDs values from the database for a set of IDs.
func (noopSession) FetchIDs(namespace ident.ID, ids ident.Iterator, startInclusive time.Time, endExclusive time.Time) (encoding.SeriesIterators, error) {
	return encoding.NewSeriesIterators(nil, nil), nil
}

// FetchTagged resolves the provided query to known IDs, and fetches the data for them.
func (noopSession) FetchTagged(namespace ident.ID, q index.Query, opts index.QueryOptions) (results encoding.SeriesIterators, exhaustive bool, err error) {
	return encoding.NewSeriesIterators(nil, nil), true, nil
}

// FetchTaggedIDs resolves the provided query to known IDs.
func (noopSession) FetchTaggedIDs(namespace ident.ID, q index.Query, opts index.QueryOptions) (iter TaggedIDsIterator, exhaustive bool, err error) {
	return newTaggedIDsIterator(nil), true, nil
}

// Aggregate aggregates values from the database for the given set of constraints.
func (noopSession) Aggregate(namespace ident.ID, q index.Query, opts index.AggregationOptions) (iter AggregatedTagsIterator, exhaustive bool, err error) {
	panic("not implemented") // TODO: Implement
}

// ShardID returns the given shard for an ID for callers
// to easily discern what shard is failing when operations
// for given IDs begin failing.
func (noopSession) ShardID(id ident.ID) (uint32, error) {
	return 0, nil
}

// IteratorPools exposes the internal iterator pools used by the session to clients.
func (noopSession) IteratorPools() (encoding.IteratorPools, error) {
	return nil, nil
}

// Close the session
func (noopSession) Close() error {
	return nil
}
