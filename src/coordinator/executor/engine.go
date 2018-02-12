package executor

import (
	"context"

	"github.com/m3db/m3coordinator/storage"
)

// Engine executes a Query.
type Engine struct {
	// Used for tracking running queries.
	tracker *Tracker
	Stats   *QueryStatistics
	store   storage.Storage
}

// EngineOptions can be used to pass custom flags to engine
type EngineOptions struct {
	// AbortCh is a channel that signals when results are no longer desired by the caller.
	AbortCh <-chan bool
}

// NewEngine returns a new instance of QueryExecutor.
func NewEngine(store storage.Storage) *Engine {
	return &Engine{
		tracker: NewTracker(),
		Stats:   &QueryStatistics{},
		store:   store,
	}
}

// QueryStatistics keeps statistics related to the QueryExecutor.
type QueryStatistics struct {
	ActiveQueries          int64
	ExecutedQueries        int64
	FinishedQueries        int64
	QueryExecutionDuration int64
}

// Execute runs the query and closes the results channel onces done
func (e *Engine) Execute(ctx context.Context, query *storage.FetchQuery, opts *EngineOptions, closing <-chan bool, results chan *storage.QueryResult) {
	defer close(results)
	task, err := e.tracker.Track(query, closing)
	if err != nil {
		select {
		case results <- &storage.QueryResult{Err: err}:
		case <-opts.AbortCh:
		}
		return
	}

	defer e.tracker.DetachQuery(task.qid)

	result, err := e.store.Fetch(ctx, query, &storage.FetchOptions{
		KillChan: task.closing,
	})
	if err != nil {
		results <- &storage.QueryResult{Err: err}
		return
	}

	results <- &storage.QueryResult{FetchResult: result}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Engine) Close() error {
	return e.tracker.Close()
}
