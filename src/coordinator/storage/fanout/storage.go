package fanout

import (
	"context"

	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/storage"
)

type fanoutStorage struct {
	stores []storage.Storage
	filter filter.Storage
}

// NewStorage creates a new remote Storage instance.
func NewStorage(stores []storage.Storage, filter filter.Storage) storage.Storage {
	return &fanoutStorage{stores: stores, filter: filter}
}

func (s *fanoutStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	stores := filterStores(s.stores, s.filter, query)
	//TODO: Actual fanout
	return stores[0].Fetch(ctx, query, options)
}

func (s *fanoutStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	stores := filterStores(s.stores, s.filter, query)
	//TODO: Actual fanout
	return stores[0].Write(ctx, query)
}

func (s *fanoutStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func filterStores(stores []storage.Storage, filterPolicy filter.Storage, query storage.Query) []storage.Storage {
	filtered := make([]storage.Storage, 0)
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
