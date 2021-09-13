package promremote

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

func (p *promStorage) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, errors.New(unimplementedmessage("FetchProm"))
}

func (p *promStorage) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, errors.New(unimplementedmessage("FetchBlocks"))
}

func (p *promStorage) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, errors.New(unimplementedmessage("FetchCompressed"))
}

func (p *promStorage) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, errors.New(unimplementedmessage("SearchSeries"))
}

func (p *promStorage) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, errors.New(unimplementedmessage("CompleteTags"))
}

func (p *promStorage) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, errors.New(unimplementedmessage("QueryStorageMetadataAttributes"))
}

func unimplementedmessage(name string) string {
	return fmt.Sprintf("promStorage: %s method is not supported", name)
}
