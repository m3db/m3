// Copyright (c) 2019 Uber Technologies, Inc.
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
package proto

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestMergeProp(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 40
	parameters.Rng.Seed(seed)

	enc := NewEncoder(time.Time{}, testEncodingOptions)
	iter := NewIterator(nil, nil, testEncodingOptions).(*iterator)
	props.Property("Encoded data should be readable", prop.ForAll(func(input propTestInput) (bool, error) {
		times := make([]time.Time, 0, len(input.messages))
		currTime := time.Now()
		for range input.messages {
			currTime = currTime.Add(time.Nanosecond)
			times = append(times, currTime)
		}

		enc.Reset(currTime, 0)
		enc.SetSchema(input.schema)

		for i, m := range input.messages {
			// The encoder will mutate the message so make sure we clone it first.
			clone := dynamic.NewMessage(input.schema)
			clone.MergeFrom(m)
			cloneBytes, err := clone.Marshal()
			if err != nil {
				return false, fmt.Errorf("error marshaling proto message: %v", err)
			}

			err = enc.Encode(ts.Datapoint{Timestamp: times[i]}, xtime.Nanosecond, cloneBytes)
			if err != nil {
				return false, fmt.Errorf(
					"error encoding message: %v, schema: %s", err, input.schema.String())
			}
		}

		stream := enc.Stream()
		iter.SetSchema(input.schema)
		iter.Reset(stream)

		i := 0
		for iter.Next() {
			var (
				m                    = input.messages[i]
				dp, unit, annotation = iter.Current()
			)
			decodedM := dynamic.NewMessage(input.schema)
			require.NoError(t, decodedM.Unmarshal(annotation))

			require.Equal(t, unit, xtime.Nanosecond)
			require.True(t, times[i].Equal(dp.Timestamp))

			for _, field := range m.GetKnownFields() {
				var (
					fieldNum    = int(field.GetNumber())
					expectedVal = m.GetFieldByNumber(fieldNum)
					actualVal   = decodedM.GetFieldByNumber(fieldNum)
				)

				if !fieldsEqual(expectedVal, actualVal) {
					return false, fmt.Errorf(
						"expected %v but got %v on iteration number %d and fieldNum %d, schema %s",
						expectedVal, actualVal, i, fieldNum, input.schema)
				}
			}

			i++
		}

		if iter.Err() != nil {
			return false, fmt.Errorf(
				"iteration error: %v, schema: %s", iter.Err(), input.schema.String())
		}

		return true, nil
	}, genPropTestInputs()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
