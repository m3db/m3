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

package convert

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestUnitForM3DB(t *testing.T) {
	for u := byte(0); u <= byte(xtime.Year); u++ {
		unit := xtime.Unit(u)
		if unit == xtime.None {
			require.Equal(t, xtime.Nanosecond, UnitForM3DB(unit))
			continue
		}

		dur, err := unit.Value()
		require.NoError(t, err)

		switch {
		case time.Duration(dur)%time.Second == 0:
			require.Equal(t, xtime.Second, UnitForM3DB(unit))
			unit = xtime.Second
		case time.Duration(dur)%time.Millisecond == 0:
			require.Equal(t, xtime.Millisecond, UnitForM3DB(unit))
		case time.Duration(dur)%time.Microsecond == 0:
			require.Equal(t, xtime.Microsecond, UnitForM3DB(unit))
		default:
			require.Equal(t, xtime.Nanosecond, UnitForM3DB(unit))
		}
	}
}

func TestMakeSureAllUnitsAreChecked(t *testing.T) {
	// NB: This test is to make sure whenever new unit type was added to m3x
	// we can catch it and add to the sanitize function.
	unit := xtime.Unit(byte(xtime.Year) + 1)
	require.False(t, unit.IsValid())
}
