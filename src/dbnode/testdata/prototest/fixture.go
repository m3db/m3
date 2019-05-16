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

package prototest

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/stretchr/testify/require"
)

const (
	protoStr = `syntax = "proto3";
package mainpkg;

message TestMessage {
  double latitude = 1;
  double longitude = 2;
  int64 epoch = 3;
  bytes deliveryID = 4;
  map<string, string> attributes = 5;
}
`
)

type TestMessage struct {
	timestamp  time.Time
	latitude   float64
	longitude  float64
	epoch      int64
	deliveryID []byte
	attributes map[string]string
}

func NewSchemaHistory() namespace.SchemaHistory {
	tempDir, err := ioutil.TempDir("", "m3dbnode-prototest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	testProtoFile := filepath.Join(tempDir, "test.proto")
	err = ioutil.WriteFile(testProtoFile, []byte(protoStr), 0666)
	if err != nil {
		panic(err)
	}

	schemaHis, err := namespace.LoadSchemaHistory(namespace.GenTestSchemaOptions(testProtoFile))
	if err != nil {
		panic(err)
	}
	return schemaHis
}

func NewMessageDescriptor(his namespace.SchemaHistory) *desc.MessageDescriptor {
	schema, ok := his.GetLatest()
	if !ok {
		panic("schema history is empty")
	}
	return schema.Get().MessageDescriptor
}

func NewProtoTestMessages(md *desc.MessageDescriptor) []*dynamic.Message {
	testFixtures := []TestMessage{
		{
			latitude:  0.1,
			longitude: 1.1,
			epoch:     -1,
		},
		{
			latitude:   0.1,
			longitude:  1.1,
			epoch:      0,
			deliveryID: []byte("123123123123"),
			attributes: map[string]string{"key1": "val1"},
		},
		{
			latitude:   0.2,
			longitude:  2.2,
			epoch:      1,
			deliveryID: []byte("789789789789"),
			attributes: map[string]string{"key1": "val1"},
		},
		{
			latitude:   0.3,
			longitude:  2.3,
			epoch:      2,
			deliveryID: []byte("123123123123"),
		},
		{
			latitude:   0.4,
			longitude:  2.4,
			epoch:      3,
			attributes: map[string]string{"key1": "val1"},
		},
		{
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
			latitude:   0.6,
			longitude:  2.6,
			deliveryID: nil,
		},
		{
			latitude:   0.5,
			longitude:  2.5,
			deliveryID: []byte("789789789789"),
		},
	}

	msgs := make([]*dynamic.Message, len(testFixtures))
	for i := 0; i < len(msgs); i++ {
		newMessage := dynamic.NewMessage(md)
		newMessage.SetFieldByName("latitude", testFixtures[i].latitude)
		newMessage.SetFieldByName("longitude", testFixtures[i].longitude)
		newMessage.SetFieldByName("deliveryID", testFixtures[i].deliveryID)
		newMessage.SetFieldByName("epoch", testFixtures[i].epoch)
		newMessage.SetFieldByName("attributes", testFixtures[i].attributes)
		msgs[i] = newMessage
	}

	return msgs
}

func RequireEqual(t *testing.T, md *desc.MessageDescriptor, expected, actual []byte) {
	expectedMsg := dynamic.NewMessage(md)
	require.NoError(t, expectedMsg.Unmarshal(expected))
	actualMsg := dynamic.NewMessage(md)
	require.NoError(t, actualMsg.Unmarshal(actual))

	require.Equal(t, expectedMsg.GetFieldByName("latitude"),
		actualMsg.GetFieldByName("latitude"))
	require.Equal(t, expectedMsg.GetFieldByName("longitude"),
		actualMsg.GetFieldByName("longitude"))
	require.Equal(t, expectedMsg.GetFieldByName("deliveryID"),
		actualMsg.GetFieldByName("deliveryID"))
	require.Equal(t, expectedMsg.GetFieldByName("epoch"),
		actualMsg.GetFieldByName("epoch"))
	requireAttributesEqual(t, expectedMsg.GetFieldByName("attributes").(map[interface{}]interface{}),
		actualMsg.GetFieldByName("attributes").(map[interface{}]interface{}))
}

func requireAttributesEqual(t *testing.T, expected, actual map[interface{}]interface{}) {
	require.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		require.Equal(t, v, actual[k])
	}
}

func ProtoEqual(md *desc.MessageDescriptor, expected, actual []byte) bool {
	expectedMsg := dynamic.NewMessage(md)
	if expectedMsg.Unmarshal(expected) != nil {
		return false
	}
	actualMsg := dynamic.NewMessage(md)
	if actualMsg.Unmarshal(actual) != nil {
		return false
	}

	if expectedMsg.GetFieldByName("latitude") !=
		actualMsg.GetFieldByName("latitude") {
		return false
	}
	if expectedMsg.GetFieldByName("longitude") !=
		actualMsg.GetFieldByName("longitude") {
		return false
	}
	if !bytes.Equal(expectedMsg.GetFieldByName("deliveryID").([]byte),
		actualMsg.GetFieldByName("deliveryID").([]byte)) {
		return false
	}
	if expectedMsg.GetFieldByName("epoch") !=
		actualMsg.GetFieldByName("epoch") {
		return false
	}
	return attributesEqual(expectedMsg.GetFieldByName("attributes").(map[interface{}]interface{}),
		actualMsg.GetFieldByName("attributes").(map[interface{}]interface{}))
}

func attributesEqual(expected, actual map[interface{}]interface{}) bool {
	if len(expected) != len(actual) {
		return false
	}
	for k, v := range expected {
		if v.(string) != actual[k].(string) {
			return false
		}
	}
	return true
}

type ProtoMessageIterator struct {
	messages []*dynamic.Message
	i        int
}

func NewProtoMessageIterator(messages []*dynamic.Message) *ProtoMessageIterator {
	return &ProtoMessageIterator{messages: messages}
}

func (pmi *ProtoMessageIterator) Next() []byte {
	n := pmi.messages[pmi.i%len(pmi.messages)]
	pmi.i++
	mbytes, err := n.Marshal()
	if err != nil {
		panic(err.Error())
	}
	return mbytes
}

func (pmi *ProtoMessageIterator) Reset() {
	pmi.i = 0
}
