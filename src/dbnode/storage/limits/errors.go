// Copyright (c) 2020 Uber Technologies, Inc.
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

package limits

import xerrors "github.com/m3db/m3/src/x/errors"

type queryLimitExceededError struct {
	msg string
}

// NewQueryLimitExceededError creates a query limit exceeded error.
func NewQueryLimitExceededError(msg string) error {
	return &queryLimitExceededError{
		msg: msg,
	}
}

func (err *queryLimitExceededError) Error() string {
	return err.msg
}

// IsQueryLimitExceededError returns true if the error is a query limits exceeded error.
func IsQueryLimitExceededError(err error) bool {
	//nolint:errorlint
	for err != nil {
		if _, ok := err.(*queryLimitExceededError); ok {
			return true
		}
		if multiErr, ok := err.(xerrors.MultiError); ok {
			for _, e := range multiErr.Errors() {
				if IsQueryLimitExceededError(e) {
					return true
				}
			}
		}
		err = xerrors.InnerError(err)
	}
	return false
}
