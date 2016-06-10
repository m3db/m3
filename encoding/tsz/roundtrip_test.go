package tsz

import (
	"math/rand"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func generateDatapoints(numPoints int, timeUnit time.Duration) []memtsdb.Datapoint {
	rand.Seed(time.Now().UnixNano())
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := 1.0
	res := []memtsdb.Datapoint{{currentTime, currentValue}}
	for i := 1; i < numPoints; i++ {
		currentTime := currentTime.Add(time.Second * time.Duration(rand.Intn(7200)))
		currentValue += (rand.Float64() - 0.5) * 10
		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, memtsdb.Datapoint{Timestamp: currentTime, Value: currentValue})
	}
	return res
}

func TestRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		input := generateDatapoints(numPoints, timeUnit)
		encoder := NewEncoder(testStartTime, nil, nil)
		for j, v := range input {
			if j == 0 {
				encoder.Encode(v, xtime.Millisecond, proto.EncodeVarint(10))
			} else if j == 10 {
				encoder.Encode(v, xtime.Millisecond, proto.EncodeVarint(60))
			} else {
				encoder.Encode(v, xtime.Second, nil)
			}
		}
		decoder := NewDecoder(nil)
		it := decoder.Decode(encoder.Stream())
		var decompressed []memtsdb.Datapoint
		j := 0
		for it.Next() {
			v, _, a := it.Current()
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
