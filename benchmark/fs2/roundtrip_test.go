package fs2

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type testEntry struct {
	id        string
	timestamp int64
	value     float64
}

func TestRoundTrip(t *testing.T) {
	indexTempFile, err := ioutil.TempFile("", "indexTempFile-")
	if err != nil {
		t.Fatal(err)
	}
	indexFilePath := indexTempFile.Name()
	defer os.Remove(indexFilePath)

	dataTempFile, err := ioutil.TempFile("", "dataTempFile-")
	if err != nil {
		t.Fatal(err)
	}
	dataFilePath := dataTempFile.Name()
	defer os.Remove(dataFilePath)

	require.NoError(t, indexTempFile.Close())
	require.NoError(t, dataTempFile.Close())
	entries := []testEntry{
		{"foo", 10, 3.6},
		{"foo", 20, 4.8},
		{"bar", 22, 9.1},
		{"bar", 24, 4.2},
	}

	writer, err := NewWriter(indexFilePath, dataFilePath)
	require.NoError(t, err)

	for i := range entries {
		require.NoError(t, writer.Write(entries[i].id, entries[i].timestamp, entries[i].value))
	}
	require.NoError(t, writer.Close())

	indexFileFd, err := os.Open(indexFilePath)
	if err != nil {
		t.Fatal(err)
	}

	dataFileFd, err := os.Open(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(indexFileFd, dataFileFd)
	require.NoError(t, err)
	iter := reader.Iter()
	index := 0
	for iter.Next() {
		id, timestamp, value := iter.Value()
		require.Equal(t, entries[index].id, id)
		require.Equal(t, entries[index].timestamp, timestamp)
		require.Equal(t, entries[index].value, value)
		index++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, len(entries), index)

	indexFileFd.Close()
	dataFileFd.Close()
}
