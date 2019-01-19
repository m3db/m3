package errors

import (
	errs "errors"
	"fmt"
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

// EncodingError wraps encoding errors for type checking
type EncodingError struct {
	ID    string
	cause error
}

func (e *EncodingError) Error() string {
	return fmt.Sprintf("Error encoding: %s [%s]", e.ID, e.cause.Error())
}

// NewEncodingError creates new EncodingError
func NewEncodingError(ID string, cause error) *EncodingError {
	return &EncodingError{
		ID:    ID,
		cause: cause,
	}
}

// IsEncodingError checks if an error is an encoding error
func IsEncodingError(err error) bool {
	if _, ok := err.(*EncodingError); ok {
		return true
	}
	return false
}
