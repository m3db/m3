package remote

import (
	"context"

	"github.com/m3db/m3coordinator/storage"
)

type remoteStorage struct {

}

// NewStorage creates a new remote Storage instance.
func NewStorage() storage.Storage {
	return &remoteStorage{}
}

func (s *remoteStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	return nil, nil
}

func (s *remoteStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return nil
}

func (s *remoteStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}
