// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tsz

import (
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func generateDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	rand.Seed(time.Now().UnixNano())
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := 1.0
	res := []ts.Datapoint{{currentTime, currentValue}}
	for i := 1; i < numPoints; i++ {
		currentTime := currentTime.Add(time.Second * time.Duration(rand.Intn(7200)))
		currentValue += (rand.Float64() - 0.5) * 10
		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, ts.Datapoint{Timestamp: currentTime, Value: currentValue})
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
				encoder.Encode(v, xtime.Microsecond, proto.EncodeVarint(60))
			} else {
				encoder.Encode(v, xtime.Second, nil)
			}
		}
		decoder := NewDecoder(nil)
		it := decoder.Decode(encoder.Stream())
		var decompressed []ts.Datapoint
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
