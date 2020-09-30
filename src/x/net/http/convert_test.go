// Copyright (c) 2018 Uber Technologies, Inc.
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

package xhttp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDurationToNanosBytes(t *testing.T) {
	type ret struct {
		output    string
		shouldErr bool
	}
	testCases := map[string]ret{
		`{"field":"value"}`:                                            ret{`{"field":"value"}`, false},
		`{"fieldDuration":"1s"}`:                                       ret{`{"fieldNanos":1000000000}`, false},
		`{"fieldDuration":1234}`:                                       ret{`{"fieldNanos":1234}`, false},
		`{"field":"value","fieldDuration":"1s"}`:                       ret{`{"field":"value","fieldNanos":1000000000}`, false},
		`{"realDuration":"50ns","nanoDuration":100,"normalNanos":200}`: ret{`{"nanoNanos":100,"normalNanos":200,"realNanos":50}`, false},
		`{"field":"value","moreFields":{"innerDuration":"2ms","innerField":"innerValue"}}`: ret{`{"field":"value","moreFields":{"innerField":"innerValue","innerNanos":2000000}}`, false},
		`{"field":[{"attrs":{"fieldDuration":"0s"}}]}`:                                     ret{`{"field":[{"attrs":{"fieldNanos":0}}]}`, false},
		`not json`:                                       ret{"", true},
		`{"fieldDuration":[]}`:                           ret{"", true},
		`{"fieldDuration":{}}`:                           ret{"", true},
		`{"fieldDuration":"badDuration"}`:                ret{"", true},
		`{"fieldDuration":100.5}`:                        ret{"", true},
		`{"moreFields":{"innerDuration":"badDuration"}}`: ret{"", true},
	}

	for k, v := range testCases {
		output, err := DurationToNanosBytes(strings.NewReader(k))
		if v.shouldErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		if output != nil {
			assert.Equal(t, v.output, string(output))
		}
	}
}

func TestNanoToDurationBytes(t *testing.T) {
	type ret struct {
		output    map[string]interface{}
		shouldErr bool
	}
	testCases := map[string]ret{
		`{"field":"value"}`:                                         ret{map[string]interface{}{"field": "value"}, false},
		`{"fieldNanos":1000000000}`:                                 ret{map[string]interface{}{"fieldDuration": "1s"}, false},
		`{"fieldNanos":0}`:                                          ret{map[string]interface{}{"fieldDuration": "0s"}, false},
		`{"field":"value","fieldNanos":1000000000}`:                 ret{map[string]interface{}{"field": "value", "fieldDuration": "1s"}, false},
		`{"realNanos":50,"nanoNanos":100,"normalDuration":"200ns"}`: ret{map[string]interface{}{"nanoDuration": "100ns", "normalDuration": "200ns", "realDuration": "50ns"}, false},
		`{"field":"value","moreFields":{"innerNanos":2000000,"innerField":"innerValue"}}`: ret{map[string]interface{}{"field": "value", "moreFields": map[string]interface{}{"innerField": "innerValue", "innerDuration": "2ms"}}, false},
		`{"field":[{"attrs":{"fieldNanos":0}}]}`:                                          ret{map[string]interface{}{"field": []interface{}{map[string]interface{}{"attrs": map[string]interface{}{"fieldDuration": "0s"}}}}, false},
		`not json`:                                                                        ret{nil, true},
		`{"fieldNanos":[]}`:                                                               ret{nil, true},
		`{"fieldNanos":{}}`:                                                               ret{nil, true},
		`{"fieldNanos":"badNanos"}`:                                                       ret{nil, true},
		`{"fieldNanos":100.5}`:                                                            ret{nil, true},
		`{"moreFields":{"innerNanos":"badNanos"}}`:                                        ret{nil, true},
	}

	for k, v := range testCases {
		output, err := NanosToDurationBytes(strings.NewReader(k))
		if v.shouldErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		if output != nil {
			if !reflect.DeepEqual(v.output, output) {
				t.Errorf("expected = %v, actual %v", v, output)
			}
		}
	}
}
