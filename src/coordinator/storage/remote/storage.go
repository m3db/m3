package remote

import (
	"context"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/tsdb/remote"
)

type remoteStorage struct {
	client remote.Client
}

// NewStorage creates a new remote Storage instance.
func NewStorage(c remote.Client) storage.Storage {
	return &remoteStorage{client: c}
}

func (s *remoteStorage) Fetch(ctx context.Context, query *storage.ReadQuery) (*storage.FetchResult, error) {
	return s.client.Fetch(ctx, query)
}

func (s *remoteStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return s.client.Write(ctx, query)
}

func (s *remoteStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}
