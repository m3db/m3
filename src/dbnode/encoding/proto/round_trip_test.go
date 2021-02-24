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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

var (
	testVLSchema  = newVLMessageDescriptor()
	testVL2Schema = newVL2MessageDescriptor()
	bytesPool     = pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
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
func TestRoundTrip(t *testing.T) {
	testCases := []struct {
		timestamp  time.Time
		unit       xtime.Unit
		latitude   float64
		longitude  float64
		epoch      int64
		deliveryID []byte
		attributes map[string]string
	}{
		{
			unit:      xtime.Second,
			latitude:  0.1,
			longitude: 1.1,
			epoch:     -1,
		},
		{
			unit:       xtime.Nanosecond,
			latitude:   0.1,
			longitude:  1.1,
			epoch:      0,
			deliveryID: []byte("123123123123"),
			attributes: map[string]string{"key1": "val1"},
		},
		{
			unit:       xtime.Nanosecond,
			latitude:   0.2,
			longitude:  2.2,
			epoch:      1,
			deliveryID: []byte("789789789789"),
			attributes: map[string]string{"key1": "val1"},
		},
		{
			unit:       xtime.Millisecond,
			latitude:   0.3,
			longitude:  2.3,
			epoch:      2,
			deliveryID: []byte("123123123123"),
		},
		{
			unit:       xtime.Second,
			latitude:   0.4,
			longitude:  2.4,
			epoch:      3,
			attributes: map[string]string{"key1": "val1"},
		},
		{
			unit:       xtime.Second,
			latitude:   0.5,
			longitude:  2.5,
			epoch:      4,
			deliveryID: []byte("456456456456"),
			attributes: map[string]string{
				"key1": "val1",
				"key2": "val2",
			},
		},
		{
			unit:       xtime.Millisecond,
			latitude:   0.6,
			longitude:  2.6,
			deliveryID: nil,
		},
		{
			unit:       xtime.Nanosecond,
			latitude:   0.5,
			longitude:  2.5,
			deliveryID: []byte("789789789789"),
		},
	}

	curr := time.Now().Truncate(2 * time.Minute)
	enc := newTestEncoder(curr)
	enc.SetSchema(namespace.GetTestSchemaDescr(testVLSchema))

	for i, tc := range testCases {
		vl := newVL(
			tc.latitude, tc.longitude, tc.epoch, tc.deliveryID, tc.attributes)
		marshalledVL, err := vl.Marshal()
		require.NoError(t, err)

		duration, err := xtime.DurationFromUnit(tc.unit)
		require.NoError(t, err)
		currTime := curr.Add(time.Duration(i) * duration)
		testCases[i].timestamp = currTime
		// Encoder should ignore value so we set it to make sure it gets ignored.
		err = enc.Encode(ts.Datapoint{Timestamp: currTime, Value: float64(i)}, tc.unit, marshalledVL)
		require.NoError(t, err)

		lastEncoded, err := enc.LastEncoded()
		require.NoError(t, err)
		require.True(t, currTime.Equal(lastEncoded.Timestamp))
		require.True(t, currTime.Equal(lastEncoded.Timestamp))
		require.Equal(t, float64(0), lastEncoded.Value)
	}

	// Add some sanity to make sure that the compression (especially string compression)
	// is working properly.
	numExpectedBytes := 281
	require.Equal(t, numExpectedBytes, enc.Stats().CompressedBytes)

	rawBytes, err := enc.Bytes()
	require.NoError(t, err)
	require.Equal(t, numExpectedBytes, len(rawBytes))

	r := xio.NewBytesReader64(rawBytes)
	iter := NewIterator(r, namespace.GetTestSchemaDescr(testVLSchema), testEncodingOptions)

	i := 0
	for iter.Next() {
		var (
			tc                   = testCases[i]
			dp, unit, annotation = iter.Current()
		)
		m := dynamic.NewMessage(testVLSchema)
		require.NoError(t, m.Unmarshal(annotation))

		require.Equal(t, unit, testCases[i].unit)
		require.True(t,
			tc.timestamp.Equal(dp.Timestamp),
			fmt.Sprintf("expected: %s, got: %s", tc.timestamp.String(), dp.Timestamp.String()))
		// Value is meaningless for proto so should always be zero
		// regardless of whats written.
		require.Equal(t, float64(0), dp.Value)
		require.Equal(t, tc.unit, unit)
		require.Equal(t, tc.latitude, m.GetFieldByName("latitude"))
		require.Equal(t, tc.longitude, m.GetFieldByName("longitude"))
		require.Equal(t, tc.epoch, m.GetFieldByName("epoch"))
		require.Equal(t, tc.deliveryID, m.GetFieldByName("deliveryID"))
		assertAttributesEqual(t, tc.attributes, m.GetFieldByName("attributes").(map[interface{}]interface{}))
		i++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, len(testCases), i)
}

func TestRoundTripMidStreamSchemaChanges(t *testing.T) {
	enc := newTestEncoder(time.Now().Truncate(time.Second))
	enc.SetSchema(namespace.GetTestSchemaDescr(testVLSchema))

	attrs := map[string]string{"key1": "val1"}
	vl1Write := newVL(26.0, 27.0, 10, []byte("some_delivery_id"), attrs)
	marshalledVL, err := vl1Write.Marshal()
	require.NoError(t, err)

	vl1WriteTime := time.Now().Truncate(time.Second)
	err = enc.Encode(ts.Datapoint{Timestamp: vl1WriteTime}, xtime.Second, marshalledVL)
	require.NoError(t, err)

	vl2Write := newVL2(28.0, 29.0, attrs, "some_new_custom_field", map[int]int{1: 2})
	marshalledVL, err = vl2Write.Marshal()
	require.NoError(t, err)

	vl2WriteTime := vl1WriteTime.Add(time.Second)
	err = enc.Encode(ts.Datapoint{Timestamp: vl2WriteTime}, xtime.Second, marshalledVL)
	require.EqualError(t,
		err,
		"proto encoder: error unmarshalling message: encountered unknown field with field number: 6")

	enc.SetSchema(namespace.GetTestSchemaDescr(testVL2Schema))
	err = enc.Encode(ts.Datapoint{Timestamp: vl2WriteTime}, xtime.Second, marshalledVL)
	require.NoError(t, err)

	rawBytes, err := enc.Bytes()
	require.NoError(t, err)

	// Try reading the stream just using the vl1 schema.
	r := xio.NewBytesReader64(rawBytes)
	iter := NewIterator(r, namespace.GetTestSchemaDescr(testVLSchema), testEncodingOptions)

	require.True(t, iter.Next(), "iter err: %v", iter.Err())
	dp, unit, annotation := iter.Current()
	m := dynamic.NewMessage(testVLSchema)
	require.NoError(t, m.Unmarshal(annotation))
	require.Equal(t, xtime.Second, unit)
	require.Equal(t, vl1WriteTime, dp.Timestamp)
	require.Equal(t, 5, len(m.GetKnownFields()))
	require.Equal(t, vl1Write.GetFieldByName("latitude"), m.GetFieldByName("latitude"))
	require.Equal(t, vl1Write.GetFieldByName("longitude"), m.GetFieldByName("longitude"))
	require.Equal(t, vl1Write.GetFieldByName("epoch"), m.GetFieldByName("epoch"))
	require.Equal(t, vl1Write.GetFieldByName("deliveryID"), m.GetFieldByName("deliveryID"))
	require.Equal(t, vl1Write.GetFieldByName("attributes"), m.GetFieldByName("attributes"))

	require.True(t, iter.Next(), "iter err: %v", iter.Err())
	dp, unit, annotation = iter.Current()
	m = dynamic.NewMessage(testVLSchema)
	require.NoError(t, m.Unmarshal(annotation))
	require.Equal(t, xtime.Second, unit)
	require.Equal(t, vl2WriteTime, dp.Timestamp)
	require.Equal(t, 5, len(m.GetKnownFields()))
	require.Equal(t, vl2Write.GetFieldByName("latitude"), m.GetFieldByName("latitude"))
	require.Equal(t, vl2Write.GetFieldByName("longitude"), m.GetFieldByName("longitude"))
	require.Equal(t, vl1Write.GetFieldByName("attributes"), m.GetFieldByName("attributes"))
	// vl2 doesn't contain these fields so they should have default values when they're
	// decoded with a vl1 schema.
	require.Equal(t, int64(0), m.GetFieldByName("epoch"))
	require.Equal(t, []byte(nil), m.GetFieldByName("deliveryID"))
	require.Equal(t, vl2Write.GetFieldByName("attributes"), m.GetFieldByName("attributes"))

	require.False(t, iter.Next())
	require.NoError(t, iter.Err())

	// Try reading the stream just using the vl2 schema.
	r = xio.NewBytesReader64(rawBytes)
	iter = NewIterator(r, namespace.GetTestSchemaDescr(testVL2Schema), testEncodingOptions)

	require.True(t, iter.Next(), "iter err: %v", iter.Err())
	dp, unit, annotation = iter.Current()
	m = dynamic.NewMessage(testVL2Schema)
	require.NoError(t, m.Unmarshal(annotation))
	require.Equal(t, xtime.Second, unit)
	require.Equal(t, vl1WriteTime, dp.Timestamp)
	require.Equal(t, 5, len(m.GetKnownFields()))
	require.Equal(t, vl1Write.GetFieldByName("latitude"), m.GetFieldByName("latitude"))
	require.Equal(t, vl1Write.GetFieldByName("longitude"), m.GetFieldByName("longitude"))
	require.Equal(t, vl1Write.GetFieldByName("attributes"), m.GetFieldByName("attributes"))
	// This field does not exist in VL1 so it should have a default value when decoding
	// with a VL2 schema.
	require.Equal(t, "", m.GetFieldByName("new_custom_field"))

	// These fields don't exist in the vl2 schema so they should not be in the returned message.
	_, err = m.TryGetFieldByName("epoch")
	require.Error(t, err)
	_, err = m.TryGetFieldByName("deliveryID")
	require.Error(t, err)

	require.True(t, iter.Next(), "iter err: %v", iter.Err())
	dp, unit, annotation = iter.Current()
	m = dynamic.NewMessage(testVL2Schema)
	require.NoError(t, m.Unmarshal(annotation))
	require.Equal(t, xtime.Second, unit)
	require.Equal(t, vl2WriteTime, dp.Timestamp)
	require.Equal(t, 5, len(m.GetKnownFields()))
	require.Equal(t, vl2Write.GetFieldByName("latitude"), m.GetFieldByName("latitude"))
	require.Equal(t, vl2Write.GetFieldByName("longitude"), m.GetFieldByName("longitude"))
	require.Equal(t, vl2Write.GetFieldByName("new_custom_field"), m.GetFieldByName("new_custom_field"))
	require.Equal(t, vl2Write.GetFieldByName("attributes"), m.GetFieldByName("attributes"))

	// These fields don't exist in the vl2 schema so they should not be in the returned message.
	_, err = m.TryGetFieldByName("epoch")
	require.Error(t, err)
	_, err = m.TryGetFieldByName("deliveryID")
	require.Error(t, err)

	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
}

func newTestEncoder(t time.Time) *Encoder {
	e := NewEncoder(t, testEncodingOptions)
	e.Reset(t, 0, nil)

	return e
}

func newVL(
	lat, long float64,
	epoch int64,
	deliveryID []byte,
	attributes map[string]string,
) *dynamic.Message {
	newMessage := dynamic.NewMessage(testVLSchema)
	newMessage.SetFieldByName("latitude", lat)
	newMessage.SetFieldByName("longitude", long)
	newMessage.SetFieldByName("deliveryID", deliveryID)
	newMessage.SetFieldByName("epoch", epoch)
	newMessage.SetFieldByName("attributes", attributes)

	return newMessage
}

func newVL2(
	lat, long float64,
	attributes map[string]string,
	newCustomField string,
	newProtoField map[int]int,
) *dynamic.Message {
	newMessage := dynamic.NewMessage(testVL2Schema)

	newMessage.SetFieldByName("latitude", lat)
	newMessage.SetFieldByName("longitude", long)
	newMessage.SetFieldByName("attributes", attributes)
	newMessage.SetFieldByName("new_custom_field", newCustomField)
	newMessage.SetFieldByName("new_proto_field", newProtoField)

	return newMessage
}

func newVLMessageDescriptor() *desc.MessageDescriptor {
	return newVLMessageDescriptorFromFile("./testdata/vehicle_location.proto")
}

func newVL2MessageDescriptor() *desc.MessageDescriptor {
	return newVLMessageDescriptorFromFile("./testdata/vehicle_location_schema_change.proto")
}

func newVLMessageDescriptorFromFile(protoSchemaPath string) *desc.MessageDescriptor {
	fds, err := protoparse.Parser{}.ParseFiles(protoSchemaPath)
	if err != nil {
		panic(err)
	}

	vlMessage := fds[0].FindMessage("VehicleLocation")
	if vlMessage == nil {
		panic(errors.New("could not find VehicleLocation message in first file"))
	}

	return vlMessage
}

func assertAttributesEqual(t *testing.T, expected map[string]string, actual map[interface{}]interface{}) {
	require.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		require.Equal(t, v, actual[k].(string))
	}
}
