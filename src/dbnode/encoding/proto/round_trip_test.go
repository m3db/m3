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
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

var (
	testVLSchema = newVLMessageDescriptor()
	bytesPool    = pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	testEncodingOptions = encoding.NewOptions().
				SetDefaultTimeUnit(xtime.Second).
				SetBytesPool(bytesPool)
)

func init() {
	bytesPool.Init()
}

// TestRoundTrip is intentionally simple to facilitate fast and easy debugging of changes
// as well as to serve as a basic sanity test. However, the bulk of the confidence in this
// code's correctness comes from the `TestRoundtripProp` test which is much more exhaustive.
// TODO: Add test for schema changes mid stream: https://github.com/m3db/m3/issues/1471
func TestRoundTrip(t *testing.T) {
	testCases := []struct {
		timestamp  time.Time
		latitude   float64
		longitude  float64
		epoch      int64
		deliveryID []byte
	}{
		{
			latitude:  0.1,
			longitude: 1.1,
			epoch:     -1,
		},
		{
			latitude:   0.1,
			longitude:  1.1,
			epoch:      0,
			deliveryID: []byte("123"),
		},
		{
			latitude:   0.2,
			longitude:  2.2,
			epoch:      1,
			deliveryID: []byte("789"),
		},
		{
			latitude:   0.3,
			longitude:  2.3,
			epoch:      2,
			deliveryID: []byte("123"),
		},
		{
			latitude:  0.4,
			longitude: 2.4,
			epoch:     3,
		},
		{
			latitude:   0.5,
			longitude:  2.5,
			epoch:      4,
			deliveryID: []byte("456"),
		},
		{
			latitude:   0.6,
			longitude:  2.6,
			deliveryID: nil,
		},
		{
			latitude:   0.5,
			longitude:  2.5,
			deliveryID: []byte("ASDFAJSDFHAJKSFHK"),
		},
	}

	enc := newTestEncoder(time.Now().Truncate(time.Second))
	enc.SetSchema(testVLSchema)

	for i, tc := range testCases {
		vl := newVL(tc.latitude, tc.longitude, tc.epoch, tc.deliveryID)
		marshaledVL, err := vl.Marshal()
		require.NoError(t, err)

		currTime := time.Now().Truncate(time.Second).Add(time.Duration(i) * time.Second)
		testCases[i].timestamp = currTime
		err = enc.Encode(ts.Datapoint{Timestamp: currTime}, xtime.Second, marshaledVL)
		require.NoError(t, err)
	}

	rawBytes, err := enc.Bytes()
	require.NoError(t, err)

	buff := bytes.NewBuffer(rawBytes)
	iter := NewIterator(buff, testVLSchema, testEncodingOptions)

	i := 0
	for iter.Next() {
		var (
			tc                   = testCases[i]
			dp, unit, annotation = iter.Current()
		)
		m := dynamic.NewMessage(testVLSchema)
		require.NoError(t, m.Unmarshal(annotation))

		require.Equal(t, unit, xtime.Second)
		require.True(t, tc.timestamp.Equal(dp.Timestamp))
		require.Equal(t, xtime.Second, unit)
		require.Equal(t, tc.latitude, m.GetFieldByName("latitude"))
		require.Equal(t, tc.longitude, m.GetFieldByName("longitude"))
		require.Equal(t, tc.epoch, m.GetFieldByName("epoch"))
		require.Equal(t, tc.deliveryID, m.GetFieldByName("deliveryID"))
		i++
	}
	require.Equal(t, len(testCases), i)
	require.NoError(t, iter.Err())
}

func newTestEncoder(t time.Time) *Encoder {
	e := NewEncoder(t, testEncodingOptions)
	e.Reset(t, 0)

	return e
}

func newVL(lat, long float64, epoch int64, deliveryID []byte) *dynamic.Message {
	newMessage := dynamic.NewMessage(testVLSchema)
	newMessage.SetFieldByName("latitude", lat)
	newMessage.SetFieldByName("longitude", long)
	newMessage.SetFieldByName("deliveryID", deliveryID)
	newMessage.SetFieldByName("epoch", epoch)

	return newMessage
}

func newVLMessageDescriptor() *desc.MessageDescriptor {
	fds, err := protoparse.Parser{}.ParseFiles("./vehicle_location.proto")
	if err != nil {
		panic(err)
	}

	vlMessage := fds[0].FindMessage("VehicleLocation")
	if vlMessage == nil {
		panic(errors.New("could not find VehicleLocation message in first file"))
	}

	return vlMessage
}
