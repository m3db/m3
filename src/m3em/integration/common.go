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

package integration

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3em/agent"

	"github.com/stretchr/testify/require"
)

const (
	rngSeed            = int64(123456789)
	defaultUniqueBytes = int64(1024 * 8)
)

type testProgram []byte

var (
	// nolint: varcheck
	shortLivedTestProgram = testProgram([]byte(`#!/usr/bin/env bash
if [ "$#" -ne 2 ]; then
	echo "Args: $@" >&2
	echo "Illegal number of arguments" >&2
	exit 1
fi
echo -ne "testing random output"`))

	// nolint: varcheck
	longRunningTestProgram = testProgram([]byte(`#!/usr/bin/env bash
	echo -ne "testing random output"
	while true; do sleep 1; done
	echo -ne "should never get this"`))
)

func newTempFile(t *testing.T, dir string, content []byte) *os.File {
	tmpfile, err := ioutil.TempFile(dir, "example")
	require.NoError(t, err)
	n, err := tmpfile.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.NoError(t, tmpfile.Close())
	return tmpfile
}

func newTestScript(t *testing.T, dir string, scriptNum int, script testProgram) string {
	file, err := ioutil.TempFile(dir, fmt.Sprintf("testscript%d.sh", scriptNum))
	require.NoError(t, err)
	name := file.Name()
	require.NoError(t, file.Chmod(0755))
	scriptContents := []byte(script)
	numWritten, err := file.Write(scriptContents)
	require.NoError(t, err)
	require.Equal(t, len(scriptContents), numWritten)
	require.NoError(t, file.Close())
	return name
}

func newLargeTempFile(t *testing.T, dir string, numBytes int64) *os.File {
	if numBytes%defaultUniqueBytes != 0 {
		require.FailNow(t, "numBytes must be divisble by %d", defaultUniqueBytes)
	}
	byteStream := newRandByteStream(t, defaultUniqueBytes)
	numIters := numBytes / defaultUniqueBytes
	tmpfile, err := ioutil.TempFile(dir, "example-large-file")
	require.NoError(t, err)
	for i := int64(0); i < numIters; i++ {
		n, err := tmpfile.Write(byteStream)
		require.NoError(t, err)
		require.Equal(t, len(byteStream), n)
	}
	require.NoError(t, tmpfile.Close())
	return tmpfile
}

func newTempDir(t *testing.T) string {
	path, err := ioutil.TempDir("", "integration-test")
	require.NoError(t, err)
	return path
}

func newSubDir(t *testing.T, dir, subdir string) string {
	newPath := path.Join(dir, subdir)
	err := os.Mkdir(newPath, os.FileMode(0755))
	require.NoError(t, err)
	return newPath
}

func newRandByteStream(t *testing.T, numBytes int64) []byte {
	if mod8 := numBytes % int64(8); mod8 != 0 {
		require.FailNow(t, "numBytes must be divisible by 8")
	}
	var (
		buff = bytes.NewBuffer(nil)
		num  = numBytes / 8
		r    = rand.New(rand.NewSource(rngSeed))
	)
	for i := int64(0); i < num; i++ {
		n := r.Int63()
		err := binary.Write(buff, binary.LittleEndian, n)
		require.NoError(t, err)
	}
	return buff.Bytes()
}

// returns true if agent finished, false otherwise
func waitUntilAgentFinished(a agent.Agent, timeout time.Duration) bool {
	start := time.Now()
	seenRunning := false
	stopped := false
	for !stopped {
		now := time.Now()
		if now.Sub(start) > timeout {
			return stopped
		}
		running := a.Running()
		seenRunning = seenRunning || running
		stopped = seenRunning && !running
		time.Sleep(10 * time.Millisecond)
	}
	return stopped
}

func testExecGenFn(binary string, config string) (string, []string) {
	return binary, []string{"-f", config}
}
