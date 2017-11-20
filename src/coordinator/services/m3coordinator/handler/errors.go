package handler

import (
	errs "errors"
	"net/http"
)

// New returns an error that formats as the given text
func New(name string) error {
	return errs.New(name)
}

type containedError struct {
	inner error
}

type containedErr interface {
	innerError() error
}

type renamedError struct {
	containedError
	renamed error
}

// NewRenamedError returns a new error that packages an inner error with a renamed error
func NewRenamedError(inner, renamed error) error {
	return renamedError{containedError{inner}, renamed}
}

func (e renamedError) Error() string {
	return e.renamed.Error()
}

func (e renamedError) innerError() error {
	return e.inner
}

// InnerError returns the packaged inner error if this is an error that contains another
func InnerError(err error) error {
	contained, ok := err.(containedErr)
	if !ok {
		return nil
	}
	return contained.innerError()
}

type retryableError struct {
	containedError
}

func (e retryableError) Error() string {
	return e.inner.Error()
}

func (e retryableError) innerError() error {
	return e.inner
}

// NewRetryableError creates a new retryable error
func NewRetryableError(inner error) error {
	return retryableError{containedError{inner}}
}

// IsRetryable indicates whether an error is a retryable error
func IsRetryable(err error) bool {
	return GetInnerRetryableError(err) != nil
}

// GetInnerRetryableError returns an inner retryable error
// if contained by this error, nil otherwise
func GetInnerRetryableError(err error) error {
	for err != nil {
		if _, ok := err.(retryableError); ok {
			return InnerError(err)
		}
		err = InnerError(err)
	}
	return nil
}

type invalidParamsError struct {
	containedError
}

// NewInvalidParamsError creates a new invalid params error
func NewInvalidParamsError(inner error) error {
	return invalidParamsError{containedError{inner}}
}

func (e invalidParamsError) Error() string {
	return e.inner.Error()
}

func (e invalidParamsError) innerError() error {
	return e.inner
}

// IsInvalidParams returns true if this is an invalid params error
func IsInvalidParams(err error) bool {
	return GetInnerInvalidParamsError(err) != nil
}

// GetInnerInvalidParamsError returns an inner invalid params error
// if contained by this error, nil otherwise
func GetInnerInvalidParamsError(err error) error {
	for err != nil {
		if _, ok := err.(invalidParamsError); ok {
			return InnerError(err)
		}
		err = InnerError(err)
	}
	return nil
}

type deprecatedError struct {
	containedError
}

// NewDeprecatedError creates a new deprecated error
func NewDeprecatedError(inner error) error {
	return deprecatedError{containedError{inner}}
}

func (e deprecatedError) Error() string {
	return e.inner.Error()
}

func (e deprecatedError) innerError() error {
	return e.inner
}

// IsDeprecated returns true if this is a deprecated error
func IsDeprecated(err error) bool {
	return GetInnerDeprecatedError(err) != nil
}

// GetInnerDeprecatedError returns an inner deprecated error
// if contained by this error, nil otherwise
func GetInnerDeprecatedError(err error) error {
	for err != nil {
		if _, ok := err.(deprecatedError); ok {
			return InnerError(err)
		}
		err = InnerError(err)
	}
	return nil
}

// CostLimitError is the error returned when cost limits are exceeded
type CostLimitError struct {
	containedError
}

// NewCostLimitError creates a new cost limit error
func NewCostLimitError(inner error) error {
	return CostLimitError{containedError{inner}}
}

func (e CostLimitError) Error() string {
	return e.inner.Error()
}

func (e CostLimitError) innerError() error {
	return e.inner
}

// IsCostLimit returns true if this is a cost limit error
func IsCostLimit(err error) bool {
	return GetInnerCostLimitError(err) != nil
}

// GetInnerCostLimitError returns an inner cost limit error
// if one is contained by this error, nil otherwise
func GetInnerCostLimitError(err error) error {
	for err != nil {
		if _, ok := err.(CostLimitError); ok {
			return InnerError(err)
		}
		err = InnerError(err)
	}
	return nil
}


// Error will server an HTTP error, if the error x/errors/.IsInvalidInputParams() or
// packages any such error it will serve the error with a bad request status code 400, otherwise
// it will serve it with an internal server status code 500.
func Error(w http.ResponseWriter, err error, code int) {
	if invalidParamsErr := GetInnerInvalidParamsError(err); invalidParamsErr != nil {
		http.Error(w, invalidParamsErr.Error(), http.StatusBadRequest)
		return
	}
	if retryHeader := w.Header().Get(RetryHeader); retryHeader == "" {
		// If the retry header is not set then notify clients that they can
		// retry this request as this is likely an intermittent server error
		w.Header().Set(RetryHeader, err.Error())
	}
	http.Error(w, err.Error(), code)
}

