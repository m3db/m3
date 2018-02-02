package fanout

import (
	"context"

	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/storage"
)

type fanoutStorage struct {
	stores      []storage.Storage
	readFilter  filter.Querier
	writeFilter filter.Appender
}

// NewStorage creates a new remote Storage instance.
func NewStorage(stores []storage.Storage, readFilter filter.Querier, writeFilter filter.Appender) storage.Storage {
	return &fanoutStorage{stores: stores, readFilter: readFilter, writeFilter: writeFilter}
}

func (s *fanoutStorage) Fetch(ctx context.Context, query *storage.ReadQuery) (*storage.FetchResult, error) {
	stores := filterReadStores(s.stores, s.readFilter, query)
	//TODO: Actual fanout
	return stores[0].Fetch(ctx, query)
}

func (s *fanoutStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	stores := filterWriteStores(s.stores, s.writeFilter, query)
	//TODO: Actual fanout
	return stores[0].Write(ctx, query)
}

func (s *fanoutStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func filterReadStores(stores []storage.Storage, filterPolicy filter.Querier, query *storage.ReadQuery) []storage.Storage {
	filtered := make([]storage.Storage, 0)
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func filterWriteStores(stores []storage.Storage, filterPolicy filter.Appender, query *storage.WriteQuery) []storage.Storage {
	filtered := make([]storage.Storage, 0)
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
