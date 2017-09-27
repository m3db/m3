// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package r2

// ConflictError represents either a version mismatch writing data or a data conflict issue.
type ConflictError struct{ msg string }

// NewConflictError creates a new Conflict Error
func NewConflictError(msg string) ConflictError { return ConflictError{msg: msg} }

func (e ConflictError) Error() string { return e.msg }

// InternalError represents an unexpected server error.
type InternalError struct{ msg string }

// NewInternalError creates a new Internal Error
func NewInternalError(msg string) InternalError { return InternalError{msg: msg} }

func (e InternalError) Error() string { return e.msg }

// VersionError represents a mismatch in the Namespaces or Ruleset version specified in the request
// and the latest one.
type VersionError struct{ msg string }

// NewVersionError creates a new Version Error
func NewVersionError(msg string) VersionError { return VersionError{msg: msg} }

func (e VersionError) Error() string { return e.msg }

// BadInputError represents an error due to malformed or invalid metrics.
type BadInputError struct{ msg string }

// NewBadInputError creates a new Bad Input Error.
func NewBadInputError(msg string) BadInputError { return BadInputError{msg: msg} }

func (e BadInputError) Error() string { return e.msg }

// NotFoundError represents an error due to malformed or invalid metrics.
type NotFoundError struct{ msg string }

// NewNotFoundError creates a new not found Error.
func NewNotFoundError(msg string) NotFoundError { return NotFoundError{msg: msg} }

func (e NotFoundError) Error() string { return e.msg }
