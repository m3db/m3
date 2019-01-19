package native

import (
	// "context"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	// "github.com/m3db/m3/src/query/storage"
)

// The Engine for running queries.
type Engine struct {
	// opts    *storage.FetchOptions
	storage storage.Storage
}

// NewEngine creates a new query engine.
func NewEngine(store storage.Storage) *Engine {
	// opts := &storage.FetchOptions{
	// 	Limit:     0,
	// 	BlockType: models.TypeDecodedBlock,
	// }

	return &Engine{
		// opts:    opts,
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
	// TODO: convert xctx.Context to context.Context
	// ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	// defer cancel()
	// // TODO: convert query string to model matchers here.
	// matchers := models.Matchers{}
	// // TODO: get the step size
	// stepSize := time.Second * 10

	// q := &storage.FetchQuery{
	// 	Raw:         query,
	// 	TagMatchers: matchers,
	// 	Start:       start,
	// 	End:         end,
	// 	Interval:    stepSize,
	// }

	// m3FetchResult, err := e.storage.Fetch(ctx, q, e.opts)
	// if err != nil {
	// 	return nil, err
	// }

	// return graphite.ConvertToGraphiteTS(xctx, start, m3FetchResult)
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

// Compile compiles an expression from an expression string
func (e *Engine) Compile(s string) (Expression, error) {
	return compile(s)
}
