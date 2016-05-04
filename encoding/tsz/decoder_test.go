package tsz

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecodeNoAnnotation(t *testing.T) {
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
		v, a := it.Current()
		require.Nil(t, a)
		results = append(results, v)
	}
	require.NoError(t, it.Err())
	require.Equal(t, expected, results)
}

func TestDecodeWithAnnotation(t *testing.T) {
	decoder := NewDecoder(time.Second)
	inputBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x0,
		0xa0, 0x90, 0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x28, 0x3f, 0x2f,
		0xc0, 0x1, 0xf4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2, 0x7, 0x10, 0x1e,
		0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	expected := []struct {
		dp  encoding.Datapoint
		ant encoding.Annotation
	}{
		{encoding.Datapoint{startTime, 12}, []byte{0xa}},
		{encoding.Datapoint{startTime.Add(time.Second * 60), 12}, nil},
		{encoding.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{encoding.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{encoding.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{encoding.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{encoding.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}
	it := decoder.Decode(bytes.NewReader(inputBytes))
	i := 0
	for it.Next() {
		v, a := it.Current()
		require.Equal(t, expected[i].ant, a)
		require.Equal(t, expected[i].dp, v)
		i++
	}
	require.NoError(t, it.Err())
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
		v, a := it.Current()
		require.Nil(t, a)
		results = append(results, v)
	}
	require.Error(t, it.Err())
}
