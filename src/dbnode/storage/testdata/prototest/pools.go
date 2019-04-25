package prototest

import (
	"io"
	"time"

	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/dbnode/encoding"
	xtime "github.com/m3db/m3/src/x/time"

)

var (
	ProtoPools = newPools()
)

type Pools struct {
	EncoderPool encoding.EncoderPool
	ReaderIterPool encoding.ReaderIteratorPool
	MultiReaderIterPool encoding.MultiReaderIteratorPool
}

func newPools() Pools {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	testEncodingOptions := encoding.NewOptions().
		SetDefaultTimeUnit(xtime.Second).
		SetBytesPool(bytesPool)

	encoderPool := encoding.NewEncoderPool(nil)
	readerIterPool := encoding.NewReaderIteratorPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := testEncodingOptions.SetEncoderPool(encoderPool)

	var timeZero time.Time
	encoderPool.Init(func() encoding.Encoder {
		return proto.NewEncoder(timeZero, encodingOpts)
	})
	readerIterPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return proto.NewIterator(r, encodingOpts)
	})
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		i := readerIterPool.Get()
		i.Reset(r)
		return i
	})

	return Pools {
		EncoderPool: encoderPool,
		ReaderIterPool: readerIterPool,
		MultiReaderIterPool: multiReaderIteratorPool,
	}
}
