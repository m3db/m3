package fs

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenFilesFails(t *testing.T) {
	testFilePath := "/not/a/real/path"
	expectedErr := errors.New("synthetic error")

	opener := func(filePath string) (*os.File, error) {
		assert.Equal(t, filePath, testFilePath)
		return nil, expectedErr
	}

	var fd *os.File
	err := openFilesWithFilePathPrefix(opener, map[string]**os.File{
		testFilePath: &fd,
	})
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestCloseFilesFails(t *testing.T) {
	file, err := ioutil.TempFile("", "testfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(file.Name())

	assert.NoError(t, file.Close())
	assert.Error(t, closeFiles(file))
}
