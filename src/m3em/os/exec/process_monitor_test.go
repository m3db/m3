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

package exec

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "temp-dir-prefix-")
	require.NoError(t, err)
	return dir
}

// nolint: unparam
func newTestScript(t *testing.T, dir string, scriptNum int, scriptContents []byte) string {
	file, err := ioutil.TempFile(dir, fmt.Sprintf("testscript%d.sh", scriptNum))
	require.NoError(t, err)
	name := file.Name()
	require.NoError(t, file.Chmod(0755))
	numWritten, err := file.Write(scriptContents)
	require.NoError(t, err)
	require.Equal(t, len(scriptContents), numWritten)
	require.NoError(t, file.Close())
	return name
}

func TestSimpleExec(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
	echo -ne "testing random output"`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{},
		OutputDir: tempDir,
	}

	success := false
	tl := NewProcessListener(
		func() { success = true },
		func(err error) { require.FailNow(t, "unexpected error: %s", err.Error()) },
	)
	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	pmStruct, ok := pm.(*processMonitor)
	require.True(t, ok)
	pmStruct.startFn = pmStruct.startSync
	require.NoError(t, pm.Start())
	require.NoError(t, pm.Err())
	require.True(t, success)

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents)
}

func TestEnvSet(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
if [ "$NEW_ENV_VAR" != "expected_arg" ]; then
	echo "Invalid value for NEW_ENV_VAR: $NEW_ENV_VAR" >&2
	exit 1
fi
echo -ne "testing random output"`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{testScript},
		OutputDir: tempDir,
		Env:       map[string]string{"NEW_ENV_VAR": "expected_arg"},
	}

	success := false
	tl := NewProcessListener(
		func() { success = true },
		func(err error) { require.FailNow(t, "unexpected error", err.Error()) },
	)
	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	pmStruct, ok := pm.(*processMonitor)
	require.True(t, ok)
	pmStruct.startFn = pmStruct.startSync
	require.NoError(t, pm.Start())
	require.NoError(t, pm.Err())
	require.True(t, success)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents, string(stderrContents))

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)
}

func TestArgPassing(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
if [ "$#" -ne 1 ]; then
	echo "Invalid number of args" >&2
	exit 1
fi
arg=$1
if [ "$arg" != "expected_arg" ]; then
	echo "Invalid value for arg: $arg" >&2
	exit 1
fi
echo -ne "testing random output"`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{testScript, "expected_arg"},
		OutputDir: tempDir,
	}

	success := false
	tl := NewProcessListener(
		func() { success = true },
		func(err error) { require.FailNow(t, "unexpected error", err.Error()) },
	)
	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	pmStruct, ok := pm.(*processMonitor)
	require.True(t, ok)
	pmStruct.startFn = pmStruct.startSync
	require.NoError(t, pm.Start())
	require.NoError(t, pm.Err())
	require.True(t, success)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	require.Equal(t, expectedStderrFile, pm.StderrPath())
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents, string(stderrContents))

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	require.Equal(t, expectedStdoutFile, pm.StdoutPath())
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)
}

func TestStderrOutput(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
	echo -ne "testing random output" >&2`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{},
		OutputDir: tempDir,
	}

	success := false
	tl := NewProcessListener(
		func() { success = true },
		func(err error) { require.FailNow(t, "unexpected error: %v", err) },
	)
	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	pmStruct, ok := pm.(*processMonitor)
	require.True(t, ok)
	pmStruct.startFn = pmStruct.startSync
	require.NoError(t, pm.Start())
	require.NoError(t, pm.Err())
	require.True(t, success)

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	require.Equal(t, expectedStdoutFile, pm.StdoutPath())
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stdoutContents)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	require.Equal(t, expectedStderrFile, pm.StderrPath())
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stderrContents)
}

func TestFailingExec(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
	echo -ne "testing random output"
	exit 1`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{},
		OutputDir: tempDir,
	}

	success := false
	tl := NewProcessListener(
		func() { require.FailNow(t, "unexpected successful execution") },
		func(err error) { success = true },
	)
	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	pmStruct, ok := pm.(*processMonitor)
	require.True(t, ok)
	pmStruct.startFn = pmStruct.startSync
	pm.Start()
	require.Error(t, pm.Err())
	require.True(t, success)

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	require.Equal(t, expectedStdoutFile, pm.StdoutPath())
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	require.Equal(t, expectedStderrFile, pm.StderrPath())
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents)
}

func TestStop(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	scriptNum := 0
	scriptContents := []byte(`#!/usr/bin/env bash
	echo -ne "testing random output"
	while true; do sleep 1; done
	echo -ne "should never get this"`)
	testScript := newTestScript(t, tempDir, scriptNum, scriptContents)
	basePath := filepath.Base(testScript)
	cmd := Cmd{
		Path:      testScript,
		Args:      []string{},
		OutputDir: tempDir,
	}
	tl := NewProcessListener(
		func() { require.FailNow(t, "unexpected OnComplete notification") },
		func(err error) { require.FailNow(t, "unexpected error: %s", err.Error()) },
	)

	pm, err := NewProcessMonitor(cmd, tl)
	require.NoError(t, err)
	require.NoError(t, pm.Start())

	// wait until execution has started
	time.Sleep(100 * time.Millisecond)
	seenRunning := false
	for !seenRunning {
		if pm.Running() {
			seenRunning = true
		}
		time.Sleep(100 * time.Millisecond)
	}

	// now crash the program
	err = pm.Stop()
	require.NoError(t, err)

	// give bg routines a chance to finish
	time.Sleep(time.Millisecond)
	require.NoError(t, pm.Err())

	expectedStdoutFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStdoutSuffix))
	require.Equal(t, expectedStdoutFile, pm.StdoutPath())
	stdoutContents, err := ioutil.ReadFile(expectedStdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)

	expectedStderrFile := path.Join(tempDir, fmt.Sprintf("%s.%s", basePath, defaultStderrSuffix))
	require.Equal(t, expectedStderrFile, pm.StderrPath())
	stderrContents, err := ioutil.ReadFile(expectedStderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents)
}
