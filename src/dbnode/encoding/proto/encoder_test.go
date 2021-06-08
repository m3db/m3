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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/stretchr/testify/require"
)

func TestCustomAndProtoFields(t *testing.T) {
	testCases := []struct {
		schema                  *desc.MessageDescriptor
		expectedCustomFields    []customFieldState
		expectedNonCustomFields []marshalledField
	}{
		{
			schema: newVLMessageDescriptor(),
			expectedCustomFields: []customFieldState{
				// latitude
				{
					fieldNum:       1,
					fieldType:      float64Field,
					protoFieldType: dpb.FieldDescriptorProto_TYPE_DOUBLE,
				},
				// longitude
				{
					fieldNum:       2,
					fieldType:      float64Field,
					protoFieldType: dpb.FieldDescriptorProto_TYPE_DOUBLE,
				},
				// numTrips
				{
					fieldNum:       3,
					fieldType:      signedInt64Field,
					protoFieldType: dpb.FieldDescriptorProto_TYPE_INT64,
				},
				// deliveryID
				{
					fieldNum:       4,
					fieldType:      bytesField,
					protoFieldType: dpb.FieldDescriptorProto_TYPE_BYTES,
				},
			},
			expectedNonCustomFields: []marshalledField{{fieldNum: 5}},
		},
	}

	for _, tc := range testCases {
		tszFields, nonCustomFields := customAndNonCustomFields(nil, nil, tc.schema)
		require.Equal(t, tc.expectedCustomFields, tszFields)
		require.Equal(t, tc.expectedNonCustomFields, nonCustomFields)
	}
}

func TestClosedEncoderIsNotUsable(t *testing.T) {
	enc := newTestEncoder(xtime.Now().Truncate(time.Second))
	enc.Close()

	err := enc.Encode(ts.Datapoint{}, xtime.Second, nil)
	require.Equal(t, errEncoderClosed, err)

	_, err = enc.LastEncoded()
	require.Equal(t, errEncoderClosed, err)
}

func TestEncoderIsNotCorruptedByInvalidWrites(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	start := xtime.Now().Truncate(time.Second)
	enc := newTestEncoder(start)
	enc.SetSchema(namespace.GetTestSchemaDescr(testVLSchema))

	vl := newVL(1.0, 2.0, 3, []byte("some-delivery-id"), nil)
	vlBytes, err := vl.Marshal()
	require.NoError(t, err)

	dp := ts.Datapoint{TimestampNanos: start.Add(time.Second)}
	err = enc.Encode(dp, xtime.Second, vlBytes)
	require.NoError(t, err)

	bytesBeforeBadWrite := getCurrEncoderBytes(ctx, t, enc)

	dp = ts.Datapoint{TimestampNanos: start.Add(2 * time.Second)}
	err = enc.Encode(dp, xtime.Second, []byte("not-valid-proto"))
	require.Error(t, err)

	bytesAfterBadWrite := getCurrEncoderBytes(ctx, t, enc)
	// Ensure that the encoder detect that the protobuf message was invalid
	// before writing any data.
	require.Equal(t, bytesBeforeBadWrite, bytesAfterBadWrite)
}

func getCurrEncoderBytes(ctx context.Context, t *testing.T, enc *Encoder) []byte {
	stream, ok := enc.Stream(ctx)
	require.True(t, ok)

	currSeg, err := stream.Segment()
	require.NoError(t, err)

	result := append([]byte(nil), currSeg.Head.Bytes()...)
	if currSeg.Tail != nil {
		result = append(result, currSeg.Tail.Bytes()...)
	}
	return result
}
