// Copyright (c) 2020 Uber Technologies, Inc.
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

package wide

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var _ QueryIterator = (*wideQueryIterator)(nil)

type wideQueryIterator struct {
	blockStart time.Time
	shards     []uint32

	fixedBufferMgr FixedBufferManager
	iters          chan *wideQueryShardIterator

	writeIter  *wideQueryShardIterator
	writeShard uint32

	readIter *wideQueryShardIterator

	state wideQueryIteratorStateShared
	opts  Options
}

type wideQueryIteratorStateShared struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

const (
	shardNotSet = math.MaxUint32
	shardEOF    = shardNotSet - 1
)

// NewQueryIterator builds a wide query iterator.
func NewQueryIterator(
	blockStart time.Time,
	shards []uint32,
	opts Options,
) QueryIterator {
	return &wideQueryIterator{
		fixedBufferMgr: newFixedBufferManager(opts),
		iters:          make(chan *wideQueryShardIterator, len(shards)),
		writeShard:     shardNotSet,
		opts:           opts,
	}
}

func (i *wideQueryIterator) SetDoneError(err error) {
	i.state.Lock()
	i.state.err = err
	i.state.Unlock()

	i.setDone()
}

func (i *wideQueryIterator) setDone() {
	i.writeShard = shardEOF
	if i.writeIter != nil {
		// Finalize the last iter for writing.
		i.writeIter.setDone()
		i.writeIter = nil
	}
	close(i.iters)
}

func (i *wideQueryIterator) ShardIter(shard uint32) (QueryShardIterator, error) {
	if i.writeShard == shard {
		return i.writeIter, nil
	}

	// Make sure progressing in shard ascending order.
	if i.writeShard != shardNotSet && shard < i.writeShard {
		if i.writeShard == shardEOF {
			return nil, fmt.Errorf(
				"shard progression already complete: attempted_next=%d",
				shard)
		}
		return nil, fmt.Errorf(
			"shard progression must be ascending: curr=%d, next=%d",
			i.writeShard, shard)
	}

	nextShardIter := newQueryShardIterator(shard, i.blockStart, i.fixedBufferMgr, i.opts)

	if i.writeIter != nil {
		// Close the current iter for writing.
		i.writeIter.setDone()
	}
	i.writeIter = nextShardIter
	i.writeShard = shard

	i.iters <- nextShardIter
	return nextShardIter, nil
}

func (i *wideQueryIterator) BlockStart() time.Time {
	return i.blockStart
}

func (i *wideQueryIterator) Shards() []uint32 {
	return i.shards
}

func (i *wideQueryIterator) Next() bool {
	iter, ok := <-i.iters
	if !ok {
		i.readIter = nil
		return false
	}

	i.readIter = iter
	return true
}

func (i *wideQueryIterator) Current() QueryShardIterator {
	return i.readIter
}

func (i *wideQueryIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryIterator) Close() {
}

const shardIterRecordsBuffer = 1024

var _ QueryShardIterator = (*wideQueryShardIterator)(nil)

type wideQueryShardIterator struct {
	shard          uint32
	blockStart     time.Time
	fixedBufferMgr FixedBufferManager

	records chan wideQueryShardIteratorQueuedRecord

	iter *wideQuerySeriesIterator

	state wideQueryShardIteratorSharedState

	opts Options
}

type wideQueryShardIteratorQueuedRecord struct {
	id                []byte
	borrowID          BorrowedBuffer
	encodedTags       []byte
	borrowEncodedTags BorrowedBuffer
	data              []byte
	borrowData        BorrowedBuffer
	metadataChecksum  int64
}

type wideQueryShardIteratorSharedState struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

func newQueryShardIterator(
	shard uint32,
	blockStart time.Time,
	fixedBufferMgr FixedBufferManager,
	opts Options,
) *wideQueryShardIterator {
	return &wideQueryShardIterator{
		shard:          shard,
		blockStart:     blockStart,
		records:        make(chan wideQueryShardIteratorQueuedRecord, shardIterRecordsBuffer),
		fixedBufferMgr: fixedBufferMgr,
		opts:           opts,
	}
}

func (i *wideQueryShardIterator) BlockStart() time.Time {
	return i.blockStart
}

func (i *wideQueryShardIterator) setDone() {
	close(i.records)
}

func (i *wideQueryShardIterator) PushRecord(r ShardIteratorRecord) error {
	var (
		qr  wideQueryShardIteratorQueuedRecord
		err error
	)

	// TODO: transactionally copy the ID + tags + anything else in one go
	// otherwise the fixed buffer manager might run out of mem and wait
	// for another buffer to be available but the existing buffer cannot
	// be released since one field here has taken a copy and needs to be
	// returned for entire underlying buffer to be released.
	qr.id, qr.borrowID, err = i.fixedBufferMgr.Copy(r.ID)
	if err != nil {
		return err
	}

	qr.encodedTags, qr.borrowEncodedTags, err = i.fixedBufferMgr.Copy(r.EncodedTags)
	if err != nil {
		return err
	}

	qr.data, qr.borrowData, err = i.fixedBufferMgr.Copy(r.Data)
	if err != nil {
		return err
	}

	qr.metadataChecksum = r.MetadataChecksum
	i.records <- qr
	return nil
}

func (i *wideQueryShardIterator) Shard() uint32 {
	return i.shard
}

func (i *wideQueryShardIterator) Next() bool {
	record, ok := <-i.records
	if !ok {
		i.iter = nil
		return false
	}

	if i.iter == nil {
		i.iter = newQuerySeriesIterator(i.opts)
	}

	i.iter.reset(record)
	return true
}

func (i *wideQueryShardIterator) Current() QuerySeriesIterator {
	return i.iter
}

func (i *wideQueryShardIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryShardIterator) Close() {
}

var _ QuerySeriesIterator = (*wideQuerySeriesIterator)(nil)

type wideQuerySeriesIterator struct {
	record      wideQueryShardIteratorQueuedRecord
	reuseableID *ident.ReuseableBytesID
	iter        encoding.ReaderIterator
	opts        Options
}

func newQuerySeriesIterator(opts Options) *wideQuerySeriesIterator {
	readerIter := opts.ReaderIteratorPool().Get()
	return &wideQuerySeriesIterator{
		reuseableID: ident.NewReuseableBytesID(),
		iter:        readerIter,
		opts:        opts,
	}
}

func (i *wideQuerySeriesIterator) reset(
	record wideQueryShardIteratorQueuedRecord,
) {
	i.record = record
	buf := bytes.NewBuffer(record.data)
	i.iter.Reset(buf, i.opts.SchemaDescr())
	i.reuseableID.Reset(i.record.id)
}

func (i *wideQuerySeriesIterator) SeriesMetadata() SeriesMetadata {
	return SeriesMetadata{
		ID:          i.record.id,
		EncodedTags: i.record.encodedTags,
	}
}

func (i *wideQuerySeriesIterator) Next() bool {
	return i.iter.Next()
}

func (i *wideQuerySeriesIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return i.iter.Current()
}

func (i *wideQuerySeriesIterator) Err() error {
	if i.iter == nil {
		return nil
	}

	return i.iter.Err()
}

func (i *wideQuerySeriesIterator) Close() {
	// Release the borrows on buffers.
	i.record.borrowID.Finalize()
	i.record.borrowData.Finalize()
	i.record.borrowEncodedTags.Finalize()
	i.record = wideQueryShardIteratorQueuedRecord{}
	// Close the read iterator. Will return it to the pool.
	i.iter.Close()
}
