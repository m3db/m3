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

package debug

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileSources(t *testing.T) {
	t.Run("Succeed", func(t *testing.T) {
		for _, name := range []string{
			"goroutine",
			"heap",
			"allocs",
			"threadcreate",
			"block",
			"mutex",
			"custom",
			"ヽ༼ຈل͜ຈ༽ﾉ ヽ༼ຈل͜ຈ༽ﾉ",
			"😍",
		} {
			for _, v := range []int{-5, -2, -1, 0, 3, 4, 5, 10, 0, 1, 2} {
				goProfSource, err := NewProfileSource(name, v)
				require.NoError(t, err)
				require.NotNil(t, goProfSource)
				require.Equal(t, goProfSource.name, name)
				require.Equal(t, goProfSource.debug, v)

				goProf := goProfSource.Profile()
				require.NotNil(t, goProf)

				// Test additional calls to Profile()
				goProf = goProfSource.Profile()
				require.NotNil(t, goProf)

				// Test zip writes
				buf := bytes.NewBuffer([]byte{})
				err = goProfSource.Write(buf, &http.Request{})
				require.NoError(t, err)
				require.NotZero(t, buf.Len())
			}
		}
	})

	t.Run("Fail", func(t *testing.T) {
		goProfSource, err := NewProfileSource("", 0)
		require.Error(t, err)
		require.Nil(t, goProfSource)
	})
}
