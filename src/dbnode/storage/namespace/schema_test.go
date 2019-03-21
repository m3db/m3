package namespace

import (
	"testing"
	"bytes"

	testproto "github.com/m3db/m3/src/dbnode/generated/proto/schema_test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestSchemaBytes() []byte {
	tm := &testproto.TestMessage{}
	bytes, _ := tm.Descriptor()
	return bytes
}

func TestSchemaToFromBytes(t *testing.T) {
	inbytes := getTestSchemaBytes()
	outschema, err := ToSchema(inbytes)
	assert.NoError(t, err)

	require.EqualValues(t, "TestMessage", outschema.Get().GetName())
	outbytes := outschema.Bytes()
	require.True(t, bytes.Equal(inbytes, outbytes))
}
