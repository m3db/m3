package proto

import (
	"os"
	"testing"

	"github.com/jhump/protoreflect/desc"
	"github.com/stretchr/testify/assert"
)

func TestParseProtoSchema(t *testing.T) {
	// Create a temporary proto file for testing
	protoContent := `syntax = "proto3";

		message TestMessage {
			string name = 1;
			int32 age = 2;
		}`

	protoFile := "test.proto"
	err := os.WriteFile(protoFile, []byte(protoContent), 0644)
	assert.NoError(t, err)
	defer func(name string) {
		_ = os.Remove(name)
	}(protoFile) // Clean up after the test

	tests := []struct {
		name         string
		filePath     string
		messageName  string
		expectErr    bool
		expectedType *desc.MessageDescriptor
	}{
		{
			name:        "Valid proto file and message",
			filePath:    protoFile,
			messageName: "TestMessage",
			expectErr:   false,
		},
		{
			name:        "Invalid proto file path",
			filePath:    "non_existent.proto",
			messageName: "TestMessage",
			expectErr:   true,
		},
		{
			name:        "Message not found",
			filePath:    protoFile,
			messageName: "UnknownMessage",
			expectErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseProtoSchema(tt.filePath, tt.messageName)
			if tt.expectErr {
				assert.Error(t, err, "Expected an error but got none")
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err, "Expected no error but got one")
				assert.NotNil(t, result, "Expected a valid MessageDescriptor but got nil")
				assert.Equal(t, tt.messageName, result.GetName())
			}
		})
	}
}
