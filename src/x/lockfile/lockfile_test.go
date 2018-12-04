package lockfile

import (
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
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
