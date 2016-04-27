package tsz

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecode(t *testing.T) {
	decoder := NewDecoder(time.Second)
	inputBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x50, 0x80, 0xf2, 0xf3, 0x2, 0x1c, 0x40, 0x7, 0x10,
		0x1e, 0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	expected := []encoding.Datapoint{
		{startTime, 12},
		{startTime.Add(time.Second * 60), 12},
		{startTime.Add(time.Second * 120), 24},
		{startTime.Add(-time.Second * 76), 24},
		{startTime.Add(-time.Second * 16), 24},
		{startTime.Add(time.Second * 2092), 15},
		{startTime.Add(time.Second * 4200), 12},
	}
	var results []encoding.Datapoint
	it := decoder.Decode(bytes.NewReader(inputBytes))
	for it.Next() {
		results = append(results, it.Value())
	}
	require.NoError(t, it.Err())
	require.Equal(t, expected, results)
}

func TestDecodeError(t *testing.T) {
	decoder := NewDecoder(time.Second)
	inputBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x50, 0x80, 0xf2, 0xf3, 0x2, 0x1c, 0x40, 0x7, 0x10,
		0x1e, 0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0,
	}
	var results []encoding.Datapoint
	it := decoder.Decode(bytes.NewReader(inputBytes))
	for it.Next() {
		results = append(results, it.Value())
	}
	require.Error(t, it.Err())
}
