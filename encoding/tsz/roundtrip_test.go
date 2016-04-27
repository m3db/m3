package tsz

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
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
		for _, v := range input {
			encoder.Encode(v)
		}
		decoder := NewDecoder(timeUnit)
		it := decoder.Decode(bytes.NewReader(encoder.Bytes()))
		var decompressed []encoding.Datapoint
		for it.Next() {
			decompressed = append(decompressed, it.Value())
		}
		require.NoError(t, it.Err())
		require.Equal(t, input, decompressed)
	}
}
