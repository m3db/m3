package fs

import (
	"io/ioutil"
	"os"
	"testing"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs/proto"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "tempFile-")
	if err != nil {
		t.Fatal(err)
	}
	filePath := tempFile.Name()
	defer os.Remove(filePath)

	require.NoError(t, tempFile.Close())
	entries := []pb.DataEntry{
		{
			Id: "foo",
			Values: []*pb.DataEntry_Datapoint{
				{Timestamp: 10, Value: 3.6},
				{Timestamp: 20, Value: 4.8},
			},
		},
		{
			Id: "bar",
			Values: []*pb.DataEntry_Datapoint{
				{Timestamp: 100, Value: 9.1},
				{Timestamp: 200, Value: 4.2},
			},
		},
	}

	writer, err := NewWriter(filePath)
	require.NoError(t, err)

	for i := range entries {
		require.NoError(t, writer.Write(&entries[i]))
	}
	require.NoError(t, writer.Close())

	reader, err := NewReader(filePath)
	require.NoError(t, err)
	iter := reader.Iter()
	for index := 0; iter.Next(); index++ {
		v := iter.Value()
		require.Equal(t, entries[index].Id, v.Id)
		require.Equal(t, entries[index].Values, v.Values)
	}
	require.NoError(t, iter.Err())
}
