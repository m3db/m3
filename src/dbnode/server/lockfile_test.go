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

package server

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAcquire(t *testing.T) {
	t.Run("process B can obtain the lock after A exits", func(t *testing.T) {
		path := tempPath()
		assert.NoError(t, newLockfileCommand(path, "", true).Run())
		_, err := os.Stat(path)
		assert.True(t, os.IsNotExist(err)) // check temp file was removed
		assert.NoError(t, newLockfileCommand(path, "", true).Run())
	})

	t.Run("process B can obtain the lock after A exits, even if A didn't remove the lock file", func(t *testing.T) {
		path := tempPath()
		assert.NoError(t, newLockfileCommand(path, "", false).Run())
		_, err := os.Stat(path)
		assert.False(t, os.IsNotExist(err)) // check temp file was *not* removed
		assert.NoError(t, newLockfileCommand(path, "", true).Run())
	})

	t.Run("if process A holds the lock, B must not be able to obtain it", func(t *testing.T) {
		path := tempPath()

		procA := newLockfileCommand(path, "1s", false)
		procB := newLockfileCommand(path, "1s", false)

		assert.NoError(t, procA.Start())
		assert.NoError(t, procB.Start())

		// one process will acquireLockfile and hold the lock, and the other will fail to acquireLockfile.
		errA, errB := procA.Wait(), procB.Wait()

		if errA != nil {
			assert.NoError(t, errB)
		} else {
			assert.Error(t, errB)
		}
	})
}

func TestCreateAndAcquire(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "TestCreateAndAcquire")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	tempSubDir := path.Join(tempDir, "testDir")

	lock, err := createAndAcquireLockfile(path.Join(tempSubDir, "testLockfile"), os.ModePerm)
	assert.NoError(t, err)
	err = lock.releaseLockfile()
	assert.NoError(t, err)

	// check createAndAcquireLockfile() created the missing directory
	_, err = os.Stat(tempSubDir)
	assert.False(t, os.IsNotExist(err))
}

// TestAcquireAndReleaseFile is invoked as a separate process by other tests in lockfile_test.go
// to exercise the file locking capabilities. The test is a no-op if run as part
// of the broader test suite. Given it's run as a separate process, we explicitly use error
// exit codes as opposed to failing assertions on errors
func TestAcquireAndReleaseFile(t *testing.T) {
	// immediately return if this test wasn't invoked by another test in the
	// nolint: goconst
	if os.Getenv("LOCKFILE_SUPERVISED_PROCESS") != "true" {
		t.Skip()
	}

	var (
		lockPath      = os.Getenv("WITH_LOCK_PATH")
		removeLock    = os.Getenv("WITH_REMOVE_LOCK")
		sleepDuration = os.Getenv("WITH_SLEEP_DURATION")
	)

	lock, err := acquireLockfile(lockPath)
	if err != nil {
		os.Exit(1)
	}

	if sleepDuration != "" {
		duration, err := time.ParseDuration(sleepDuration)
		if err != nil {
			os.Exit(1)
		}

		time.Sleep(duration)
	}

	if removeLock == "true" {
		err := lock.releaseLockfile()
		if err != nil {
			os.Exit(1)
		}
	}
}

func tempPath() string {
	return filepath.Join(os.TempDir(), "lockfile_test_"+strconv.Itoa(os.Getpid())+"_"+strconv.Itoa(rand.Intn(100000)))
}

func newLockfileCommand(lockPath string, sleepDuration string, removeLock bool) *exec.Cmd {
	removeLockStr := "false"
	if removeLock {
		removeLockStr = "true"
	}

	cmd := exec.Command("go", "test", "-run", "TestAcquireAndReleaseFile")
	cmd.Env = os.Environ()
	cmd.Env = append(
		cmd.Env,
		"LOCKFILE_SUPERVISED_PROCESS=true",
		fmt.Sprintf("WITH_LOCK_PATH=%s", lockPath),
		fmt.Sprintf("WITH_SLEEP_DURATION=%s", sleepDuration),
		fmt.Sprintf("WITH_REMOVE_LOCK=%s", removeLockStr),
	)

	return cmd
}
