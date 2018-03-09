package mock

import (
	"context"

	"github.com/m3db/m3coordinator/storage"
)

type mockStorage struct{}

// NewMockStorage creates a new mock Storage instance.
func NewMockStorage() storage.Storage {
	return &mockStorage{}
}

func (s *mockStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	return nil, nil
}

func (s *mockStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return storage.Type(0)
}
