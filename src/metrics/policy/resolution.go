// Copyright (c) 2016 Uber Technologies, Inc.
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

package policy

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/m3db/m3x/time"
)

const (
	windowPrecisionSeparator = "@"
)

var (
	emptyResolution Resolution
)

// Resolution is the sampling resolution for datapoints.
type Resolution struct {
	// Window is the bucket size represented by the resolution.
	Window time.Duration

	// Precision is the precision of datapoints stored at this resoluion.
	Precision xtime.Unit
}

// String is the string representation of a resolution.
func (r Resolution) String() string {
	return fmt.Sprintf("%s%s1%s", r.Window.String(), windowPrecisionSeparator, r.Precision.String())
}

// ParseResolution parses a resolution.
func ParseResolution(str string) (Resolution, error) {
	separatorIdx := strings.Index(str, windowPrecisionSeparator)

	// If there is no separator, the precision unit is the maximum time unit
	// for which the window size is a multiple of.
	if separatorIdx == -1 {
		windowSize, err := xtime.ParseExtendedDuration(str)
		if err != nil {
			return emptyResolution, err
		}
		_, precision, err := xtime.MaxUnitForDuration(windowSize)
		if err != nil {
			return emptyResolution, err
		}
		return Resolution{Window: windowSize, Precision: precision}, nil
	}

	// Otherwise the window and the precision are determined by the input.
	windowSize, err := xtime.ParseExtendedDuration(str[:separatorIdx])
	if err != nil {
		return emptyResolution, err
	}
	precisionDuration, err := xtime.ParseExtendedDuration(str[separatorIdx+1:])
	if err != nil {
		return emptyResolution, err
	}
	precision, err := xtime.UnitFromDuration(precisionDuration)
	if err != nil {
		return emptyResolution, err
	}
	return Resolution{Window: windowSize, Precision: precision}, nil
}

// ResolutionValue is the resolution value.
type ResolutionValue int

// List of known resolution values.
const (
	UnknownResolutionValue ResolutionValue = iota
	OneSecond
	TenSeconds
	OneMinute
	FiveMinutes
	TenMinutes
)

var (
	errUnknownResolution      = errors.New("unknown resolution")
	errUnknownResolutionValue = errors.New("unknown resolution value")

	// EmptyResolution is an empty resolution.
	EmptyResolution Resolution
)

// Resolution returns the resolution associated with a value.
func (v ResolutionValue) Resolution() (Resolution, error) {
	resolution, exists := valuesToResolution[v]
	if !exists {
		return EmptyResolution, errUnknownResolutionValue
	}
	return resolution, nil
}

// IsValid returns whether the resolution value is valid.
func (v ResolutionValue) IsValid() bool {
	_, valid := valuesToResolution[v]
	return valid
}

// ValueFromResolution returns the value given a resolution.
func ValueFromResolution(resolution Resolution) (ResolutionValue, error) {
	value, exists := resolutionToValues[resolution]
	if exists {
		return value, nil
	}
	return UnknownResolutionValue, errUnknownResolution
}

var (
	valuesToResolution = map[ResolutionValue]Resolution{
		OneSecond:   Resolution{Window: time.Second, Precision: xtime.Second},
		TenSeconds:  Resolution{Window: 10 * time.Second, Precision: xtime.Second},
		OneMinute:   Resolution{Window: time.Minute, Precision: xtime.Minute},
		FiveMinutes: Resolution{Window: 5 * time.Minute, Precision: xtime.Minute},
		TenMinutes:  Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
	}

	resolutionToValues = make(map[Resolution]ResolutionValue)
)

func init() {
	for value, resolution := range valuesToResolution {
		resolutionToValues[resolution] = value
	}
}
