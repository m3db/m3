package errors

import "errors"

var (
	// ErrHeaderNotFound is returned when a header is not found
	ErrHeaderNotFound = errors.New("header not found")
	// ErrBatchQuery is returned when a batch query is found
	ErrBatchQuery    = errors.New("batch queries are currently not supported")
	// ErrNoTargetFound is returned when a target is not found
	ErrNoTargetFound = errors.New("no target found")

)
