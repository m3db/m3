package lockfile

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	path := filepath.Join(os.TempDir(), "lockfile_test_"+strconv.Itoa(rand.Intn(100000)))

	lock, err := Create(path)
	assert.NoError(t, err, "Create() returned unexpected error")

	err = lock.Remove()
	assert.NoError(t, err, "Remove() returned unexpected error")

}
