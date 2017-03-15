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

package customtransport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTCalcTransport(t *testing.T) {
	trans := &TCalcTransport{}
	require.Nil(t, trans.Open())
	require.True(t, trans.IsOpen())
	require.EqualValues(t, 0, trans.GetCount())

	testString1 := "test"
	testString2 := "string"
	n, err := trans.Write([]byte(testString1))
	require.Equal(t, len(testString1), n)
	require.Nil(t, err)
	require.EqualValues(t, len(testString1), trans.GetCount())
	n, err = trans.Write([]byte(testString2))
	require.EqualValues(t, len(testString2), n)
	require.Nil(t, err)
	require.EqualValues(t, len(testString1)+len(testString2), trans.GetCount())

	n, err = trans.Read([]byte(testString1))
	require.Nil(t, err)
	require.EqualValues(t, 0, n)
	require.Equal(t, ^uint64(0), trans.RemainingBytes())

	trans.ResetCount()
	require.EqualValues(t, 0, trans.GetCount())

	err = trans.Flush()
	require.Nil(t, err)
	require.Nil(t, trans.Close())
}
