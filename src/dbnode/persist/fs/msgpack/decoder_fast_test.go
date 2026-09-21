package msgpack

import (
	"testing"

	"github.com/stretchr/testify/require"
	vmmsgpack "gopkg.in/vmihailenco/msgpack.v2"
	"gopkg.in/vmihailenco/msgpack.v2/codes"

	"github.com/m3db/m3/src/dbnode/persist/schema"
)

func TestDecodeLogEntryFastSuccess(t *testing.T) {
	expected := schema.LogEntry{
		Index:      123,
		Create:     456,
		Metadata:   []byte("meta"),
		Timestamp:  789,
		Value:      3.14,
		Unit:       2,
		Annotation: []byte("annot"),
	}

	var encoded []byte
	encoded = append(encoded, make([]byte, len(logEntryHeader))...)

	encode := func(v interface{}) {
		b, err := vmmsgpack.Marshal(v)
		require.NoError(t, err)
		encoded = append(encoded, b...)
	}

	encode(expected.Index)
	encode(expected.Create)
	encode(expected.Metadata)
	encode(expected.Timestamp)
	encode(expected.Value)
	encode(uint64(expected.Unit))
	encode(expected.Annotation)

	result, err := DecodeLogEntryFast(encoded)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestDecodeLogMetadataFastSuccess(t *testing.T) {
	expected := schema.LogMetadata{
		ID:          []byte("id"),
		Namespace:   []byte("ns"),
		Shard:       10,
		EncodedTags: []byte("tags"),
	}

	var encoded []byte
	encoded = append(encoded, make([]byte, len(logMetadataHeader))...)

	encode := func(v interface{}) {
		b, err := vmmsgpack.Marshal(v)
		require.NoError(t, err)
		encoded = append(encoded, b...)
	}

	encode(expected.ID)
	encode(expected.Namespace)
	encode(uint64(expected.Shard))
	encode(expected.EncodedTags)

	result, err := DecodeLogMetadataFast(encoded)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestDecodeLogEntryFastErrors(t *testing.T) {
	// Too short
	short := []byte{1, 2, 3}
	_, err := DecodeLogEntryFast(short)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough bytes")

	// Valid header but not enough data
	bad := append(make([]byte, len(logEntryHeader)), []byte{codes.Int8}...)
	_, err = DecodeLogEntryFast(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decodeUInt")
}

func TestDecodeBytesErrors(t *testing.T) {
	// Invalid code
	_, _, err := decodeBytes([]byte{0xcd}) // 205
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid code")

	// Not enough bytes
	_, _, err = decodeBytes([]byte{codes.Str8, 10, 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough bytes")
}

func TestDecodeInt(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedVal int64
		expectedRem []byte
		expectErr   bool
		errContains string
	}{
		{
			name:        "nil value",
			input:       []byte{codes.Nil, 0xAA},
			expectedVal: 0,
			expectedRem: []byte{0xAA},
		},
		{
			name:        "positive fixed number",
			input:       []byte{0x05, 0x01},
			expectedVal: 5,
			expectedRem: []byte{0x01},
		},
		{
			name:        "negative fixed number",
			input:       []byte{codes.NegFixedNumLow, 0xAB},
			expectedVal: -32,
			expectedRem: []byte{0xAB},
		},
		{
			name:        "uint8 decode",
			input:       []byte{codes.Uint8, 0x7F},
			expectedVal: 127,
			expectedRem: []byte{},
		},
		{
			name:        "int8 decode",
			input:       []byte{codes.Int8, 0x80},
			expectedVal: -128,
			expectedRem: []byte{},
		},
		{
			name:        "uint16 decode",
			input:       []byte{codes.Uint16, 0x01, 0x02},
			expectedVal: 258,
			expectedRem: []byte{},
		},
		{
			name:        "int16 decode",
			input:       []byte{codes.Int16, 0xFF, 0xFE},
			expectedVal: -2,
			expectedRem: []byte{},
		},
		{
			name:        "uint32 decode",
			input:       []byte{codes.Uint32, 0x00, 0x00, 0x01, 0x00},
			expectedVal: 256,
			expectedRem: []byte{},
		},
		{
			name:        "int32 decode",
			input:       []byte{codes.Int32, 0xFF, 0xFF, 0xFF, 0xFE},
			expectedVal: -2,
			expectedRem: []byte{},
		},
		{
			name:        "uint64 decode",
			input:       []byte{codes.Uint64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			expectedVal: 256,
			expectedRem: []byte{},
		},
		{
			name:        "int64 decode",
			input:       []byte{codes.Int64, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE},
			expectedVal: -2,
			expectedRem: []byte{},
		},
		{
			name:        "invalid code returns error",
			input:       []byte{0xC1},
			expectErr:   true,
			errContains: "error decoding int: invalid code",
		},
		{
			name:        "not enough bytes for uint8",
			input:       []byte{codes.Uint8},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 1 but had 0",
		},
		{
			name:        "not enough bytes for int8",
			input:       []byte{codes.Int8},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 1 but had 0",
		},
		{
			name:        "not enough bytes for uint16",
			input:       []byte{codes.Uint16, 0x01},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 2 but had 1",
		},
		{
			name:        "not enough bytes for int16",
			input:       []byte{codes.Int16, 0x01},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 2 but had 1",
		},
		{
			name:        "not enough bytes for uint32",
			input:       []byte{codes.Uint32, 0x01, 0x02, 0x03},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 4 but had 3",
		},
		{
			name:        "not enough bytes for int32",
			input:       []byte{codes.Int32, 0x01, 0x02, 0x03},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 4 but had 3",
		},
		{
			name:        "not enough bytes for uint64",
			input:       []byte{codes.Uint64, 0x01, 0x02, 0x03, 0x04},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 8 but had 4",
		},
		{
			name:        "not enough bytes for int64",
			input:       []byte{codes.Int64, 0x01, 0x02, 0x03, 0x04},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 8 but had 4",
		},
		{
			name:        "not enough initial bytes",
			input:       []byte{},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeInt, expected 1 but had 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, rem, err := decodeInt(tt.input)

			if tt.expectErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedVal, val)
				require.Equal(t, tt.expectedRem, rem)
			}
		})
	}
}

func TestDecodeUint(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedVal uint64
		expectedLen int
		expectErr   bool
		errContains string
	}{
		{
			name:        "Nil",
			input:       []byte{codes.Nil},
			expectedVal: 0,
			expectedLen: 0,
		},
		{
			name:        "FixedNum positive",
			input:       []byte{0x05},
			expectedVal: uint64(int8(0x05)),
			expectedLen: 0,
		},
		{
			name:        "FixedNum negative",
			input:       []byte{0xe0},
			expectedVal: 0xffffffffffffffe0,
			expectedLen: 0,
		},
		{
			name:        "Uint8",
			input:       []byte{codes.Uint8, 0x2A},
			expectedVal: 42,
			expectedLen: 1,
		},
		{
			name:        "Int8",
			input:       []byte{codes.Int8, 0xFE},
			expectedVal: 0xfffffffffffffffe,
			expectedLen: 1,
		},
		{
			name:        "Uint16",
			input:       []byte{codes.Uint16, 0x01, 0x02},
			expectedVal: 0x0102,
			expectedLen: 2,
		},
		{
			name:        "Int16",
			input:       []byte{codes.Int16, 0xFF, 0xFE},
			expectedVal: 0xfffffffffffffffe,
			expectedLen: 2,
		},
		{
			name:        "Uint32",
			input:       []byte{codes.Uint32, 0x00, 0x00, 0x00, 0x2A},
			expectedVal: 42,
			expectedLen: 4,
		},
		{
			name:        "Int32",
			input:       []byte{codes.Int32, 0xFF, 0xFF, 0xFF, 0xFE},
			expectedVal: 0xfffffffffffffffe,
			expectedLen: 4,
		},
		{
			name: "Uint64",
			input: []byte{
				codes.Uint64, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x2A,
			},
			expectedVal: 42,
			expectedLen: 8,
		},
		{
			name:        "Invalid code",
			input:       []byte{0xd0},
			expectErr:   true,
			errContains: "error decoding int: invalid code",
		},
		{
			name:        "Not enough bytes",
			input:       []byte{codes.Uint32, 0x00, 0x00}, // only 2 bytes instead of 4
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Uint8",
			input:       []byte{codes.Uint8},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Int8",
			input:       []byte{codes.Int8},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Uint16",
			input:       []byte{codes.Uint16, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Int16",
			input:       []byte{codes.Int16, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Uint32",
			input:       []byte{codes.Uint32, 0x00, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Int32",
			input:       []byte{codes.Int32, 0x00, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Uint64",
			input:       []byte{codes.Uint64, 0x00, 0x00, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
		{
			name:        "Not enough bytes for Int64",
			input:       []byte{codes.Int64, 0x00, 0x00, 0x00},
			expectErr:   true,
			errContains: "not enough bytes for msgpack decode in decodeUint",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, remaining, err := decodeUint(tc.input)

			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedVal, val)
			require.Equal(t, tc.input[1+tc.expectedLen:], remaining)
		})
	}
}

func TestDecodeBytesLenNilAndErrors(t *testing.T) {
	l, rem, err := decodeBytesLen([]byte{codes.Nil})
	require.NoError(t, err)
	require.Equal(t, -1, l)
	require.NotNil(t, rem)

	l, _, err = decodeBytesLen([]byte{0xa5})
	require.NoError(t, err)
	require.Equal(t, 5, l)

	_, _, err = decodeBytesLen([]byte{0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid code")
}

func TestDecodeArrayLen(t *testing.T) {
	// Nil
	l, _, err := decodeArrayLen([]byte{codes.Nil})
	require.NoError(t, err)
	require.Equal(t, -1, l)

	l, _, err = decodeArrayLen([]byte{0x92, 0xd2})
	require.NoError(t, err)
	require.Equal(t, 2, l)

	_, _, err = decodeArrayLen([]byte{codes.Int8})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough bytes")
}
