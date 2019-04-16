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

package log

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoggingConfiguration(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "logtest")
	require.NoError(t, err)

	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	cfg := Configuration{
		Fields: map[string]interface{}{
			"my-field": "my-val",
		},
		Level: "error",
		File:  tmpfile.Name(),
	}

	log, err := cfg.BuildLogger()
	require.NoError(t, err)

	log.Info("should not appear")
	log.Warn("should not appear")
	log.Error("this should appear")

	b, err := ioutil.ReadAll(tmpfile)
	require.NoError(t, err)

	data := string(b)
	require.Equal(t, 1, strings.Count(data, "\n"), data)
	require.True(t, strings.Contains(data, `"msg":"this should appear"`))
	require.True(t, strings.Contains(data, `"my-field":"my-val"`))
	require.True(t, strings.Contains(data, `"level":"error"`))
}
