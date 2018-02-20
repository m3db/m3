package errors

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrQueryAborted is an error returned when the query is aborted.
	ErrQueryAborted = errors.New("query aborted")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrQueryTimeoutLimitExceeded is an error when the query hits the max time allowed to run.
	ErrQueryTimeoutLimitExceeded = errors.New("query timeout limit exceeded")

	// ErrNoClientAddresses is an error when there are no addresses passed to the remote client
	ErrNoClientAddresses = errors.New("no client addresses given")
)

// ErrMaxConcurrentQueriesLimitExceeded is an error when the query cannot be run
// because the maximum number of queries has been reached.
func ErrMaxConcurrentQueriesLimitExceeded(n, limit int) error {
	return fmt.Errorf("max concurrent queries limit exceeded(%d, %d)", n, limit)
}
