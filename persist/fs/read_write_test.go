package fs

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntry struct {
	key  string
	data []byte
}

func TestSimpleReadWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", []byte{1, 2, 3}},
		{"bar", []byte{4, 5, 6}},
	}

	w := NewWriter(testWriterStart, testWindow, filePathPrefix, nil)
	err = w.Open(0)
	assert.NoError(t, err)

	for i := range entries {
		assert.NoError(t, w.Write(entries[i].key, entries[i].data))
	}
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, 0)
	assert.NoError(t, err)

	assert.Equal(t, len(entries), r.Entries())
	assert.Equal(t, 0, r.EntriesRead())

	for i := 0; i < r.Entries(); i++ {
		key, data, err := r.Read()
		assert.NoError(t, err)
		assert.Equal(t, entries[i].key, key)
		assert.True(t, bytes.Equal(entries[i].data, data))

		assert.Equal(t, i+1, r.EntriesRead())
	}

	assert.NoError(t, r.Close())
}
