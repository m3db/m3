package common

import (
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
)

// QueryEngine is the generic engine interface.
type QueryEngine interface {
	FetchByQuery(
		ctx context.Context,
		query string,
		start, end time.Time,
		localOnly, useCache, useM3DB bool,
		timeout time.Duration,
	) (*storage.FetchResult, error)
}

// The Engine for running queries
type Engine struct {
	storage storage.Storage
}

// NewEngine creates a new query engine
func NewEngine(storage storage.Storage) *Engine {
	return &Engine{
		storage: storage,
	}
}

// FetchByQuery retrieves one or more time series based on a query
func (e *Engine) FetchByQuery(
	ctx context.Context,
	query string,
	start, end time.Time,
	localOnly, useCache, useM3DB bool,
	timeout time.Duration,
) (*storage.FetchResult, error) {
	return e.storage.FetchByQuery(
		ctx,
		query,
		storage.FetchOptions{
			StartTime: start,
			EndTime:   end,
			DataOptions: storage.DataOptions{
				Timeout: timeout,
			},
		},
	)
}
