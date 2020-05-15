package prometheus

import (
	"context"
	"errors"

	"github.com/m3db/m3/src/query/storage"
)

// ContextKey is the context key type.
type ContextKey string

const (
	// FetchOptionsContextKey is the context key for fetch options.
	FetchOptionsContextKey ContextKey = "fetch-options"
)

func fetchOptions(ctx context.Context) (*storage.FetchOptions, error) {
	fetchOptions := ctx.Value(FetchOptionsContextKey)
	if f, ok := fetchOptions.(*storage.FetchOptions); ok {
		return f, nil
	}
	return nil, errors.New("fetch options not available")
}
