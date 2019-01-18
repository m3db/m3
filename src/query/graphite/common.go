package graphite

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
