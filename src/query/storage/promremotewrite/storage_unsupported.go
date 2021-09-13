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
func (p *promStorage) FetchProm(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.PromResult, error) {
	return storage.PromResult{}, errors.New(unimplementedMessage)
}

func (p *promStorage) FetchBlocks(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	return block.Result{}, errors.New(unimplementedMessage)
}

func (p *promStorage) FetchCompressed(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (consolidators.MultiFetchResult, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) SearchSeries(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) CompleteTags(ctx context.Context, query *storage.CompleteTagsQuery, options *storage.FetchOptions) (*consolidators.CompleteTagsResult, error) {
	return nil, errors.New(unimplementedMessage)
}

func (p *promStorage) QueryStorageMetadataAttributes(ctx context.Context, queryStart, queryEnd time.Time, opts *storage.FetchOptions) ([]storagemetadata.Attributes, error) {
	return nil, errors.New(unimplementedMessage)
}

