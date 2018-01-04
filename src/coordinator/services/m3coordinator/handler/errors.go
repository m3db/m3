package handler

import (
	"net/http"
)

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
