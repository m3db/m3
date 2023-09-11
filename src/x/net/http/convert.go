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
	"fmt"
	"io"
	"strconv"
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

// NanosToDurationBytes transforms a json byte slice with Nano keys into Duration keys.
func NanosToDurationBytes(r io.Reader) (map[string]interface{}, error) {
	var dict map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	if err := d.Decode(&dict); err != nil {
		return nil, fmt.Errorf("error decoding JSON: %s", err.Error())
	}

	ret, err := NanosToDurationMap(dict)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NanosToDurationMap transforms keys with Nanos into Duration.
func NanosToDurationMap(input map[string]interface{}) (map[string]interface{}, error) {
	dictTranslated := make(map[string]interface{}, len(input))

	for k, v := range input {
		if strings.HasSuffix(k, nanosSuffix) {
			newKey := strings.Replace(k, nanosSuffix, durationSuffix, 1)

			switch vv := v.(type) {
			case string:
				i, err := strconv.ParseInt(vv, 10, 64)
				if err != nil {
					return nil, err
				}

				dur := time.Duration(i) * time.Nanosecond
				dictTranslated[newKey] = dur.String()
			case json.Number:
				// json.Number when using a json decoder with UseNumber()
				// Assume given number is in nanos
				vvNum, err := vv.Int64()
				if err != nil {
					return nil, err
				}

				dur := time.Duration(vvNum) * time.Nanosecond
				dictTranslated[newKey] = dur.String()
			case float64:
				// float64 when unmarshaling without UseNumber()
				// Assume given number is in nanos
				dur := time.Duration(int64(vv)) * time.Nanosecond
				dictTranslated[newKey] = dur.String()
			default:
				// Has Nano suffix, but is not string or number.
				return nil, errDurationType
			}
		} else {
			switch vv := v.(type) {
			case map[string]interface{}:
				durMap, err := NanosToDurationMap(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[k] = durMap
			case []interface{}:
				durArr, err := nanosToDurationArray(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[k] = durArr
			default:
				dictTranslated[k] = vv
			}
		}
	}

	return dictTranslated, nil
}

func nanosToDurationArray(input []interface{}) ([]interface{}, error) {
	arrTranslated := make([]interface{}, len(input))
	for i, elem := range input {
		switch v := elem.(type) {
		case map[string]interface{}:
			durMap, err := NanosToDurationMap(v)
			if err != nil {
				return nil, err
			}

			arrTranslated[i] = durMap
		case []interface{}:
			durArr, err := nanosToDurationArray(v)
			if err != nil {
				return nil, err
			}

			arrTranslated[i] = durArr
		default:
			arrTranslated[i] = v
		}
	}

	return arrTranslated, nil
}

// DurationToNanosBytes transforms a json byte slice with Duration keys into Nanos
func DurationToNanosBytes(r io.Reader) ([]byte, error) {
	var dict map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	if err := d.Decode(&dict); err != nil {
		return nil, fmt.Errorf("error decoding JSON: %s", err.Error())
	}

	ret, err := DurationToNanosMap(dict)
	if err != nil {
		return nil, fmt.Errorf("error converting duration to nanos: %s", err.Error())
	}

	b, err := json.Marshal(ret)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %s", err.Error())
	}
	return b, nil
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
			case []interface{}:
				durArr, err := durationToNanosArray(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[k] = durArr
			default:
				dictTranslated[k] = vv
			}
		}
	}

	return dictTranslated, nil
}

func durationToNanosArray(input []interface{}) ([]interface{}, error) {
	arrTranslated := make([]interface{}, len(input))
	for i, elem := range input {
		switch v := elem.(type) {
		case map[string]interface{}:
			durMap, err := DurationToNanosMap(v)
			if err != nil {
				return nil, err
			}

			arrTranslated[i] = durMap
		case []interface{}:
			durArr, err := durationToNanosArray(v)
			if err != nil {
				return nil, err
			}

			arrTranslated[i] = durArr
		default:
			arrTranslated[i] = v
		}
	}

	return arrTranslated, nil
}
