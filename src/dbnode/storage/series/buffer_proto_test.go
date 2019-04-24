package series

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/stretchr/testify/assert"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
	"github.com/m3db/m3/src/x/pool"
)

var (
	testSchema = newVLMessageDescriptor()
)

func newBufferTestProtoOptions() Options {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	testEncodingOptions := encoding.NewOptions().
		SetDefaultTimeUnit(xtime.Second).
		SetBytesPool(bytesPool)

	encoderPool := encoding.NewEncoderPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := testEncodingOptions.SetEncoderPool(encoderPool)

	encoderPool.Init(func() encoding.Encoder {
		e := proto.NewEncoder(timeZero, encodingOpts)
		e.SetSchema(testSchema)
		return e
	})
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return proto.NewIterator(r, testSchema, encodingOpts)
	})

	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBufferBucketPool(bufferBucketPool).
		SetBufferBucketVersionsPool(bufferBucketVersionsPool)
	opts = opts.
		SetRetentionOptions(opts.RetentionOptions().
		SetBlockSize(2 * time.Minute).
		SetBufferFuture(10 * time.Second).
		SetBufferPast(10 * time.Second)).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
		SetContextPool(opts.ContextPool()).
		SetEncoderPool(opts.EncoderPool()))
	return opts
}

type vehicleData struct {
	timestamp  time.Time
	latitude   float64
	longitude  float64
	epoch      int64
	deliveryID []byte
	attributes map[string]string
}

func TestBufferProtoWriteRead(t *testing.T) {
	opts := newBufferTestProtoOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	protoData := []vehicleData{
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

	data := make([]value, len(protoData))
	for i := 0; i < len(data); i++ {
		protoBytes, err := newMessage(protoData[i]).Marshal()
		require.NoError(t, err)
		data[i] = value{
			curr.Add(time.Duration(i) * time.Second), float64(i), xtime.Second, protoBytes}
	}

	for _, v := range data {
		verifyWriteToBuffer(t, buffer, v)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func newVLMessageDescriptor() *desc.MessageDescriptor {
	return newVLMessageDescriptorFromFile("./testdata/vehicle_location.proto")
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

func newMessage(data vehicleData) *dynamic.Message {
	newMessage := dynamic.NewMessage(testSchema)
	newMessage.SetFieldByName("latitude", data.latitude)
	newMessage.SetFieldByName("longitude", data.longitude)
	newMessage.SetFieldByName("deliveryID", data.deliveryID)
	newMessage.SetFieldByName("epoch", data.epoch)
	newMessage.SetFieldByName("attributes", data.attributes)

	return newMessage
}
