package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestNullEncoder(t *testing.T) {
	encoder := NewNullEncoder()

	// Test Encode (should return nil)
	err := encoder.Encode(ts.Datapoint{}, xtime.Unit(0), nil)
	assert.NoError(t, err)

	// Test Stream (should return nil, false)
	stream, ok := encoder.Stream(context.NewBackground())
	assert.Nil(t, stream)
	assert.False(t, ok)

	// Test NumEncoded (should be 0)
	assert.Equal(t, 0, encoder.NumEncoded())

	// Test LastEncoded (should return an error)
	_, err = encoder.LastEncoded()
	assert.Error(t, err)

	// Test LastAnnotationChecksum (should return an error)
	_, err = encoder.LastAnnotationChecksum()
	assert.Error(t, err)

	// Test Empty (should return true)
	assert.True(t, encoder.Empty())

	// Test Len (should return 0)
	assert.Equal(t, 0, encoder.Len())

	// Test Discard (should return an empty segment)
	segment := encoder.Discard()
	assert.Equal(t, ts.Segment{}, segment)
}

func TestNullReaderIterator(t *testing.T) {
	iterator := NewNullReaderIterator()

	// Test Current (should return default values)
	dp, unit, annotation := iterator.Current()
	assert.Equal(t, ts.Datapoint{}, dp)
	assert.Equal(t, xtime.Unit(0), unit)
	assert.Nil(t, annotation)

	// Test Next (should return false)
	assert.False(t, iterator.Next())

	// Test Err (should return an error)
	assert.Error(t, iterator.Err())

	// Test Close (should not panic)
	iterator.Close()
}
