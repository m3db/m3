// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import "fmt"

var (
	// NB: Container behavior must not be defined in configs, so it does not count
	// as a parsable behavior. This is because it's a composite storage type that
	// refers to fanout storage, and requires further parsing to determine error
	// behavior.
	parsableErrorBehaviors = []ErrorBehavior{
		BehaviorFail,
		BehaviorWarn,
	}
)

func (e ErrorBehavior) String() string {
	switch e {
	case BehaviorFail:
		return "fail"
	case BehaviorWarn:
		return "warn"
	case BehaviorContainer:
		return "container"
	default:
		return "unknown"
	}
}

// ParseErrorBehavior parses an error behavior.
func ParseErrorBehavior(str string) (ErrorBehavior, error) {
	for _, valid := range parsableErrorBehaviors {
		if str == valid.String() {
			return valid, nil
		}
	}

	return 0, fmt.Errorf("unrecognized error behavior: %v", str)
}

// MarshalYAML marshals an ErrorBehavior.
func (e *ErrorBehavior) MarshalYAML() (interface{}, error) {
	return e.String(), nil
}

// UnmarshalYAML unmarshals an error behavior.
func (e *ErrorBehavior) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	if value, err := ParseErrorBehavior(str); err == nil {
		*e = value
		return nil
	}

	return fmt.Errorf("invalid ErrorBehavior '%s' valid types are: %v",
		str, parsableErrorBehaviors)
}

// IsWarning determines if the given error coming from the storage is a warning,
// and returns it with appropriate wrapping.
func IsWarning(store Storage, err error) (bool, error) {
	if _, ok := err.(warnError); ok {
		return true, err
	}

	if store.ErrorBehavior() == BehaviorWarn {
		return true, warnError{err}
	}

	return false, err
}

// warnError is an error that should only warn on failure.
type warnError struct {
	inner error
}

func (e warnError) Error() string {
	return e.inner.Error()
}
