package promremotewrite

import (
	"context"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

//TODO panic as error?

func (p *promStorage) FetchProm(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.PromResult, error) {
	panic("implement me")
}

func (p *promStorage) FetchBlocks(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	panic("implement me")
}

func (p *promStorage) FetchCompressed(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (consolidators.MultiFetchResult, error) {
	panic("implement me")
}

func (p *promStorage) SearchSeries(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	panic("implement me")
}

func (p *promStorage) CompleteTags(ctx context.Context, query *storage.CompleteTagsQuery, options *storage.FetchOptions) (*consolidators.CompleteTagsResult, error) {
	panic("implement me")
}

func (p *promStorage) QueryStorageMetadataAttributes(ctx context.Context, queryStart, queryEnd time.Time, opts *storage.FetchOptions) ([]storagemetadata.Attributes, error) {
	panic("implement me")
}

func (p *promStorage) Type() storage.Type {
	panic("implement me")
}

func (p *promStorage) Close() error {
	panic("implement me")
}

func (p *promStorage) ErrorBehavior() storage.ErrorBehavior {
	panic("implement me")
}

func (p *promStorage) Name() string {
	panic("implement me")
}

