package promremotewrite

import (
	"context"
	"errors"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

const unimplementedMessage = "storage method is not supported"

func (p *promStorage) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, errors.New(unimplementedMessage)
}

func (p *promStorage) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, errors.New(unimplementedMessage)
}

func (p *promStorage) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, errors.New(unimplementedMessage)
}
