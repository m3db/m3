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

package static

import (
	"fmt"
	"strings"

	"github.com/m3db/m3metrics/rules/validator/namespace"
)

// ValidationResult is the validation result.
type ValidationResult string

// A list of supported validation result.
const (
	Invalid ValidationResult = "invalid"
	Valid   ValidationResult = "valid"
)

var (
	validValidationResults = []ValidationResult{
		Invalid,
		Valid,
	}
)

// UnmarshalYAML unmarshals YAML object into a validation result.
func (t *ValidationResult) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	validResults := make([]string, 0, len(validValidationResults))
	for _, valid := range validValidationResults {
		if str == string(valid) {
			*t = valid
			return nil
		}
		validResults = append(validResults, string(valid))
	}
	return fmt.Errorf("invalid validation result '%s' valid results are: %s",
		str, strings.Join(validResults, ", "))
}

type validator struct {
	validationResult ValidationResult
}

// NewNamespaceValidator creates a new static namespace validator.
func NewNamespaceValidator(res ValidationResult) namespace.Validator {
	return &validator{validationResult: res}
}

func (v *validator) Validate(ns string) error {
	switch v.validationResult {
	case Invalid:
		return fmt.Errorf("static validator returns invalid for %s", ns)
	default:
		return nil
	}
}

func (v *validator) Close() {}
