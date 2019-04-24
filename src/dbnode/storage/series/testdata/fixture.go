package testdata

import (
	"time"

	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/jhump/protoreflect/dynamic"
	"testing"
	"github.com/stretchr/testify/require"
)

type TestMessage struct {
	timestamp  time.Time
	latitude   float64
	longitude  float64
	epoch      int64
	deliveryID []byte
	attributes map[string]string
}

var (
	TestSchemaHistory = newSchemaHistory()
	TestProtoMessages = newProtoTestMessages()
)

func newSchemaHistory() namespace.SchemaHistory {
	schemaHis, err := namespace.LoadSchemaHistory(
		namespace.GenTestSchemaOptions("main.proto", "testdata"))
	if err != nil {
		panic(err.Error())
	}
	return schemaHis
}

func newProtoTestMessages() []*dynamic.Message {
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

	testSchema, ok := TestSchemaHistory.GetLatest()
	if !ok {
		panic("test schema history is empty")
	}
	md := testSchema.Get().MessageDescriptor

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

func RequireEqual(t *testing.T, expected, actual []byte) {
	testSchema, ok := TestSchemaHistory.GetLatest()
	if !ok {
		panic("test schema history is empty")
	}
	md := testSchema.Get().MessageDescriptor

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
