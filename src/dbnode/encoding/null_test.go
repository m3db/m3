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

	err := encoder.Encode(ts.Datapoint{}, xtime.Unit(0), nil)
	assert.NoError(t, err)

	stream, ok := encoder.Stream(context.NewBackground())
	assert.Nil(t, stream)
	assert.False(t, ok)

	assert.Equal(t, 0, encoder.NumEncoded())

	_, err = encoder.LastEncoded()
	assert.Error(t, err)

	_, err = encoder.LastAnnotationChecksum()
	assert.Error(t, err)

	assert.True(t, encoder.Empty())

	assert.Equal(t, 0, encoder.Len())

	segment := encoder.Discard()
	assert.Equal(t, ts.Segment{}, segment)
}

func TestNullReaderIterator(t *testing.T) {
	iterator := NewNullReaderIterator()

	dp, unit, annotation := iterator.Current()
	assert.Equal(t, ts.Datapoint{}, dp)
	assert.Equal(t, xtime.Unit(0), unit)
	assert.Nil(t, annotation)

	assert.False(t, iterator.Next())

	assert.Error(t, iterator.Err())

	iterator.Close()
}
