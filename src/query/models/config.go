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

package models

import (
	"fmt"
)

var validIDSchemes = []IDSchemeType{
	TypeLegacy,
	TypeQuoted,
	TypePrependMeta,
}

func (v IDSchemeType) validateIDSchemeType() error {
	if v >= TypeLegacy && v <= TypePrependMeta {
		return nil
	}

	return fmt.Errorf("invalid config id schema type '%v': should be one of %v",
		v, validIDSchemes)
}

// UnmarshalYAML unmarshals a stored merics type.
// func (v *IDSchemeType) UnmarshalYAML(unmarshal func(interface{}) error) error {
// 	var str string
// 	if err := unmarshal(&str); err != nil {
// 		return err
// 	}
// 	for _, valid := range validIDSchemes {
// 		if str == valid.String() {
// 			*v = valid
// 			return nil
// 		}
// 	}
// 	return fmt.Errorf("invalid MetricsType '%s' valid types are: %v",
// 		str, validIDSchemes)
// }
