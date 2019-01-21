package native

import (
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
)

// The Engine for running queries.
type Engine struct {
	storage storage.Storage
}

// NewEngine creates a new query engine.
func NewEngine(store storage.Storage) *Engine {
	return &Engine{
		storage: store,
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
				Timeout: timeout,
			},
		},
	)
}

// Compile compiles an expression from an expression string
func (e *Engine) Compile(s string) (Expression, error) {
	return compile(s)
}
