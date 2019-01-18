package common

import (
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3coordinator/storage/fanout"
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

// The Engine for running queries.
type Engine struct {
	storage fanout.Storage
}

// NewEngine creates a new query engine.
func NewEngine(storage fanout.Storage) *Engine {
	return &Engine{
		storage: storage,
	}
}

// FetchByQuery retrieves one or more time series based on a query.
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
				LocalOnly: localOnly,
				UseCache:  useCache,
				UseM3DB:   useM3DB,
				Timeout:   timeout,
			},
		},
	)
}
