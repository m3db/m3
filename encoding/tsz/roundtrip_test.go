package tsz

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func generateDatapoints(numPoints int, timeUnit time.Duration) []encoding.Datapoint {
	rand.Seed(time.Now().UnixNano())
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := 1.0
	res := []encoding.Datapoint{{currentTime, currentValue}}
	for i := 1; i < numPoints; i++ {
		currentTime := currentTime.Add(time.Second * time.Duration(rand.Intn(7200)))
		currentValue += (rand.Float64() - 0.5) * 10
		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, encoding.Datapoint{currentTime, currentValue})
	}
	return res
}

func TestRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		input := generateDatapoints(numPoints, timeUnit)
		encoder := NewEncoder(testStartTime, timeUnit)
		for j, v := range input {
			if j == 0 {
				encoder.Encode(v, proto.EncodeVarint(10))
			} else if j == 10 {
				encoder.Encode(v, proto.EncodeVarint(60))
			} else {
				encoder.Encode(v, nil)
			}
		}
		decoder := NewDecoder(timeUnit)
		it := decoder.Decode(bytes.NewReader(encoder.Bytes()))
		var decompressed []encoding.Datapoint
		j := 0
		for it.Next() {
			v, a := it.Current()
			if j == 0 {
				s, _ := proto.DecodeVarint(a)
				require.Equal(t, uint64(10), s)
			} else if j == 10 {
				s, _ := proto.DecodeVarint(a)
				require.Equal(t, uint64(60), s)
			} else {
				require.Nil(t, a)
			}
			decompressed = append(decompressed, v)
			j++
		}
		require.NoError(t, it.Err())
		require.Equal(t, input, decompressed)
	}
}
