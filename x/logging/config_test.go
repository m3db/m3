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

package logging

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-common/bark"
)

func TestLoggingConfiguration(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "logtest")
	require.NoError(t, err)

	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	cfg := Configuration{
		Fields: bark.Fields{
			"my-field1": "my-val1",
			"my-field2": "my-val2",
		},
		Level:         "error",
		File:          tmpfile.Name(),
		DisableColors: true,
	}

	log, err := cfg.BuildLogger()
	require.NoError(t, err)

	log.Infof("should not appear")
	log.Warnf("should not appear")
	log.Errorf("this should be appear")

	b, err := ioutil.ReadAll(tmpfile)
	require.NoError(t, err)

	str := string(b)
	pieces := strings.Split(str, "\"")
	assert.True(t, len(pieces) >= 3)

	ts := pieces[1]
	assert.EqualValues(t, "time=\""+ts+"\" level=error msg=\"this should be appear\" my-field1=my-val1 my-field2=my-val2 \n", str)
}
