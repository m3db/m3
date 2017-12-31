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

import "errors"

// NewInternalError returns a new error that isn't covered by the other error types.
func NewInternalError(msg string) error { return errors.New(msg) }

// ConflictError represents either a version mismatch writing data or a data conflict issue.
type conflictError string

// NewConflictError creates a new Conflict Error
func NewConflictError(msg string) error { return conflictError(msg) }

func (e conflictError) Error() string { return string(e) }

// VersionError represents a mismatch in the Namespaces or Ruleset version specified in the request
// and the latest one.
type versionError string

// NewVersionError creates a new Version Error
func NewVersionError(msg string) error { return versionError(msg) }

func (e versionError) Error() string { return string(e) }

// BadInputError represents an error due to malformed or invalid metrics.
type badInputError string

// NewBadInputError creates a new Bad Input Error.
func NewBadInputError(msg string) error { return badInputError(msg) }

func (e badInputError) Error() string { return string(e) }

// NotFoundError represents an error due to malformed or invalid metrics.
type notFoundError string

// NewNotFoundError creates a new not found Error.
func NewNotFoundError(msg string) error { return notFoundError(msg) }

func (e notFoundError) Error() string { return string(e) }

// AuthError represents an error due to missing or invalid auth information.
type authError string

// NewAuthError creates a new not found Error.
func NewAuthError(msg string) error { return authError(msg) }

func (e authError) Error() string { return string(e) }
