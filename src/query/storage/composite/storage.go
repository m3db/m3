package composite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

type compositeStorage struct {
	name   string
	stores []storage.Storage
}

func Compose(stores ...storage.Storage) storage.Storage {
	var name strings.Builder
	for _, s := range stores {
		name.WriteString(s.Name())
		name.WriteByte('-')
	}
	name.WriteString("compositedStorage")
	return &compositeStorage{
		name:   name.String(),
		stores: stores,
	}
}

func (s *compositeStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	var finalError error
	for _, store := range s.stores {
		if err := store.Write(ctx, query); err != nil {
			finalError = err
		}
	}
	return finalError
}

func (s *compositeStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorWarn
}

func (s *compositeStorage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (s *compositeStorage) Name() string {
	return s.name
}

func (s *compositeStorage) Close() error {
	for _, store := range s.stores {
		if err := store.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *compositeStorage) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, unimplementedError(s.name, "FetchProm")
}

func (s *compositeStorage) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, unimplementedError(s.name, "FetchBlocks")
}

func (s *compositeStorage) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, unimplementedError(s.name, "FetchCompressed")
}

func (s *compositeStorage) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, unimplementedError(s.name, "SearchSeries")
}

func (s *compositeStorage) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, unimplementedError(s.name, "CompleteTags")
}

func (s *compositeStorage) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, unimplementedError(s.name, "QueryStorageMetadataAttributes")
}

func unimplementedError(storeName, name string) error {
	return fmt.Errorf("%s: %s method is not supported", storeName, name)
}
