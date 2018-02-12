package handler

import (
	"errors"
	"net/http"
	"strings"
)

// ErrInvalidParams is returned when input parameters are invalid
var ErrInvalidParams = errors.New("invalid request params")

// Error will serve an HTTP error
func Error(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}

// ParseError is the error from parsing requests
type ParseError struct {
	inner error
	code  int
}

// NewParseError creates a new parse error
func NewParseError(inner error, code int) *ParseError {
	return &ParseError{inner, code}
}

// Errors returns the error object
func (e *ParseError) Error() error {
	return e.inner
}

// Code returns the error code
func (e *ParseError) Code() int {
	return e.code
}

// IsInvalidParams returns true if this is an invalid params error
func IsInvalidParams(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), ErrInvalidParams.Error()) {
		return true
	}

	return false
}
