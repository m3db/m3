package errors

import (
	"errors"
)

var (
	// ErrNilWriteQuery is returned when trying to write a nil query
	ErrNilWriteQuery = errors.New("nil write query")

	// ErrInvalidFetchResponse is returned when fetch fails from storage.
	ErrInvalidFetchResponse = errors.New("invalid response from fetch")

	// ErrFetchResponseOrder is returned fetch responses are not in order.
	ErrFetchResponseOrder = errors.New("responses out of order for fetch")

	// ErrFetchRequestType is an error returned when response from fetch has invalid type.
	ErrFetchRequestType = errors.New("invalid request type")

	// ErrInvalidFetchResult is an error returned when fetch result is invalid.
	ErrInvalidFetchResult = errors.New("invalid fetch result")
)
