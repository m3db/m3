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

package lockfile

import (
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcquire(t *testing.T) {
	t.Run("process B can obtain the lock after A exits", func(t *testing.T) {
		path := tempPath()
		assert.NoError(t, newLockfileCommand(path, 0, true).Run())
		_, err := os.Stat(path)
		assert.True(t, os.IsNotExist(err)) // check temp file was removed
		assert.NoError(t, newLockfileCommand(path, 0, true).Run())
	})

	t.Run("process B can obtain the lock after A exits, even if A didn't remove the lock file", func(t *testing.T) {
		path := tempPath()
		assert.NoError(t, newLockfileCommand(path, 0, false).Run())
		_, err := os.Stat(path)
		assert.False(t, os.IsNotExist(err)) // check temp file was *not* removed
		assert.NoError(t, newLockfileCommand(path, 0, true).Run())
	})

	t.Run("if process A holds the lock, B must not be able to obtain it", func(t *testing.T) {
		path := tempPath()

		procA := newLockfileCommand(path, 1, true)
		procB := newLockfileCommand(path, 1, true)

		// to avoid sleeping until A obtains the lock (it takes some
		// time for the process to boot and obtain the lock), we start
		// both processes, then check exactly one of them failed
		assert.NoError(t, procA.Start())
		assert.NoError(t, procB.Start())

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

	lock, err := CreateAndAcquire(path.Join(tempSubDir, "testLockfile"), os.ModePerm)
	assert.NoError(t, err)
	err = lock.Release()
	assert.NoError(t, err)

	// check CreateAndAcquire() created the missing directory
	_, err = os.Stat(tempSubDir)
	assert.False(t, os.IsNotExist(err))
}

func tempPath() string {
	return filepath.Join(os.TempDir(), "lockfile_test_"+strconv.Itoa(os.Getpid())+"_"+strconv.Itoa(rand.Intn(100000)))
}

func newLockfileCommand(lockPath string, sleep int, removeLock bool) *exec.Cmd {
	removeLockStr := "0"
	if removeLock {
		removeLockStr = "1"
	}

	return exec.Command("go", "run", "../../../scripts/lockfile/lockfile.go", lockPath, strconv.Itoa(sleep), removeLockStr)
}
