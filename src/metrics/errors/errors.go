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

package errors

// InvalidInputError is returned when a rule or rule change is applied which
// is invalid.
type InvalidInputError string

// NewInvalidInputError creates a new invalid input error.
func NewInvalidInputError(str string) error { return InvalidInputError(str) }
func (e InvalidInputError) Error() string   { return string(e) }

// ValidationError is returned when validation failed.
type ValidationError string

// NewValidationError creates a new validation error.
func NewValidationError(str string) error { return ValidationError(str) }
func (e ValidationError) Error() string   { return string(e) }

// StaleDataError is returned when a rule modification can not be completed
// because rule metadata is no longer valid.
type StaleDataError string

// NewStaleDataError creates a new version mismatch error.
func NewStaleDataError(str string) error { return StaleDataError(str) }
func (e StaleDataError) Error() string   { return string(e) }
