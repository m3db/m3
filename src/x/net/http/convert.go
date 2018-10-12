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
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

const (
	durationSuffix = "Duration"
	nanosSuffix    = "Nanos"
)

var (
	errDurationType = errors.New("invalid duration type")
)

// DurationToNanosBytes transforms a json byte slice with Duration keys into Nanos
func DurationToNanosBytes(r io.Reader) ([]byte, error) {
	var dict map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	if err := d.Decode(&dict); err != nil {
		return nil, err
	}

	ret, err := DurationToNanosMap(dict)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ret)
}

// DurationToNanosMap transforms keys with a Duration into Nanos
func DurationToNanosMap(input map[string]interface{}) (map[string]interface{}, error) {
	dictTranslated := make(map[string]interface{}, len(input))

	for k, v := range input {
		if strings.HasSuffix(k, durationSuffix) {
			newKey := strings.Replace(k, durationSuffix, nanosSuffix, 1)

			switch vv := v.(type) {
			case string:
				duration, err := time.ParseDuration(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[newKey] = duration.Nanoseconds()
			case json.Number:
				// json.Number when using a json decoder with UseNumber()
				// Assume given number is in nanos
				vvNum, err := vv.Int64()
				if err != nil {
					return nil, err
				}

				dictTranslated[newKey] = vvNum
			case float64:
				// float64 when unmarshaling without UseNumber()
				// Assume given number is in nanos
				dictTranslated[newKey] = int64(vv)
			default:
				// Has Duration suffix, but is not string or number
				return nil, errDurationType
			}
		} else {
			switch vv := v.(type) {
			case map[string]interface{}:
				durMap, err := DurationToNanosMap(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[k] = durMap
			default:
				dictTranslated[k] = vv
			}
		}
	}

	return dictTranslated, nil
}
