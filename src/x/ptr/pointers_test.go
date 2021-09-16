package ptr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	assert.Equal(t, "string", ToString(String("string")))
	assert.Equal(t, "", ToString(nil))
}

func TestStringEqual(t *testing.T) {
	assert.True(t, StringEqual(nil, nil))
	assert.True(t, StringEqual(String("foo"), String("foo")))
	assert.False(t, StringEqual(nil, String("foo")))
	assert.False(t, StringEqual(String("foo"), nil))
	assert.False(t, StringEqual(String("foo"), String("bar")))
}

func TestIntEqual(t *testing.T) {
	assert.True(t, IntEqual(nil, nil))
	assert.True(t, IntEqual(Int(100), Int(100)))
	assert.False(t, IntEqual(nil, Int(100)))
	assert.False(t, IntEqual(Int(100), nil))
	assert.False(t, IntEqual(Int(100), Int(200)))
}

func TestInt32Equal(t *testing.T) {
	assert.True(t, Int32Equal(nil, nil))
	assert.True(t, Int32Equal(Int32(100), Int32(100)))
	assert.False(t, Int32Equal(nil, Int32(100)))
	assert.False(t, Int32Equal(Int32(100), nil))
	assert.False(t, Int32Equal(Int32(100), Int32(200)))
}

func TestInt64Equal(t *testing.T) {
	assert.True(t, Int64Equal(nil, nil))
	assert.True(t, Int64Equal(Int64(100), Int64(100)))
	assert.False(t, Int64Equal(nil, Int64(100)))
	assert.False(t, Int64Equal(Int64(100), nil))
	assert.False(t, Int64Equal(Int64(100), Int64(200)))
}

func TestBoolEqual(t *testing.T) {
	assert.True(t, BoolEqual(nil, nil))
	assert.True(t, BoolEqual(Bool(false), Bool(false)))
	assert.True(t, BoolEqual(Bool(true), Bool(true)))
	assert.False(t, BoolEqual(nil, Bool(true)))
	assert.False(t, BoolEqual(Bool(true), nil))
	assert.False(t, BoolEqual(Bool(true), Bool(false)))
}

func TestFloat64Equal(t *testing.T) {
	assert.True(t, Float64Equal(nil, nil))
	assert.True(t, Float64Equal(Float64(100.56), Float64(100.56)))
	assert.False(t, Float64Equal(nil, Float64(100.56)))
	assert.False(t, Float64Equal(Float64(100.56), nil))
	assert.False(t, Float64Equal(Float64(100.56), Float64(200.56)))
}

func TestUint(t *testing.T) {
	assert.Equal(t, uint(5), ToUInt(UInt(5)))
	assert.Equal(t, uint(0), ToUInt(UInt(0)))
	assert.Equal(t, uint(0), ToUInt(nil))
}
