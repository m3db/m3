// Copyright (c) 2016 Uber Technologies, Inc.
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

package commitlog

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	// var instead of const so we can modify them in tests.
	defaultDecodeEntryBufSize = 1024
	decoderInBufChanSize      = 1000
	decoderOutBufChanSize     = 1000
)

var (
	emptyLogInfo schema.LogInfo

	errCommitLogReaderChunkSizeChecksumMismatch = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderIsNotReusable             = errors.New("commit log reader is not reusable")
	errCommitLogReaderMultipleReadloops         = errors.New("commit log reader tried to open multiple readLoops, do not call Read() concurrently")
	errCommitLogReaderMissingMetadata           = errors.New("commit log reader encountered a datapoint without corresponding metadata")
)

// ReadAllSeriesPredicate can be passed as the seriesPredicate for callers
// that want a convenient way to read all series in the commitlogs
func ReadAllSeriesPredicate() SeriesFilterPredicate {
	return func(id ident.ID, namespace ident.ID) bool { return true }
}

type seriesMetadata struct {
	ts.Series
	passedPredicate bool
}

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (int64, error)

	// Read returns the next id and data pair or error, will return io.EOF at end of volume
	Read() (ts.Series, ts.Datapoint, xtime.Unit, ts.Annotation, error)

	// Close the reader
	Close() error
}

type readResponse struct {
	series     ts.Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
	resultErr  error
}

type decoderArg struct {
	bytes                []byte
	err                  error
	decodeRemainingToken msgpack.DecodeLogEntryRemainingToken
	uniqueIndex          uint64
	offset               int
	bufPool              chan []byte
}

type readerMetadata struct {
	sync.RWMutex
	numBlockedOrFinishedDecoders int64
}

type reader struct {
	opts                 Options
	numConc              int64
	checkedBytesPool     pool.CheckedBytesPool
	chunkReader          *chunkReader
	infoDecoder          *msgpack.Decoder
	infoDecoderStream    msgpack.DecoderStream
	decoderQueues        []chan decoderArg
	decoderBufPools      []chan []byte
	outChan              chan readResponse
	cancelCtx            context.Context
	cancelFunc           context.CancelFunc
	shutdownCh           chan error
	metadata             readerMetadata
	nextIndex            int64
	hasBeenOpened        bool
	bgWorkersInitialized int64
	seriesPredicate      SeriesFilterPredicate
}

func newCommitLogReader(opts Options, seriesPredicate SeriesFilterPredicate) commitLogReader {
	decodingOpts := opts.FilesystemOptions().DecodingOptions()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	numConc := opts.ReadConcurrency()
	decoderQueues := make([]chan decoderArg, 0, numConc)
	decoderBufs := make([]chan []byte, 0, numConc)
	for i := 0; i < numConc; i++ {
		decoderQueues = append(decoderQueues, make(chan decoderArg, decoderInBufChanSize))

		chanBufs := make(chan []byte, decoderInBufChanSize+1)
		for i := 0; i < decoderInBufChanSize+1; i++ {
			// Bufs will be resized as needed, so its ok if our default isn't big enough
			chanBufs <- make([]byte, defaultDecodeEntryBufSize)
		}
		decoderBufs = append(decoderBufs, chanBufs)
	}
	outBuf := make(chan readResponse, decoderOutBufChanSize*numConc)

	reader := &reader{
		opts:              opts,
		numConc:           int64(numConc),
		checkedBytesPool:  opts.BytesPool(),
		chunkReader:       newChunkReader(opts.FlushSize()),
		infoDecoder:       msgpack.NewDecoder(decodingOpts),
		infoDecoderStream: msgpack.NewDecoderStream(nil),
		decoderQueues:     decoderQueues,
		decoderBufPools:   decoderBufs,
		outChan:           outBuf,
		cancelCtx:         cancelCtx,
		cancelFunc:        cancelFunc,
		shutdownCh:        make(chan error),
		metadata:          readerMetadata{},
		nextIndex:         0,
		seriesPredicate:   seriesPredicate,
	}
	return reader
}

func (r *reader) Open(filePath string) (int64, error) {
	// Commitlog reader does not currently support being reused
	if r.hasBeenOpened {
		return 0, errCommitLogReaderIsNotReusable
	}
	r.hasBeenOpened = true

	fd, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}

	r.chunkReader.reset(fd)
	info, err := r.readInfo()
	if err != nil {
		r.Close()
		return 0, err
	}
	index := info.Index

	return index, nil
}

// Read guarantees that the datapoints it returns will be in the same order as they are on disk
// for a given series, but they will not be in the same order they are on disk across series.
// I.E, if the commit log looked like this (letters are series and numbers are writes):
// A1, B1, B2, A2, C1, D1, D2, A3, B3, D2
// Then the caller is guaranteed to receive A1 before A2 and A2 before A3, and they are guaranteed
// to see B1 before B2, but they may see B1 before A1 and D2 before B3.
func (r *reader) Read() (
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
	resultErr error,
) {
	if r.nextIndex == 0 {
		err := r.startBackgroundWorkers()
		if err != nil {
			return ts.Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), err
		}
	}
	rr, ok := <-r.outChan
	if !ok {
		return ts.Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), io.EOF
	}
	r.nextIndex++
	return rr.series, rr.datapoint, rr.unit, rr.annotation, rr.resultErr
}

func (r *reader) startBackgroundWorkers() error {
	// Make sure background workers are never setup more than once
	set := atomic.CompareAndSwapInt64(&r.bgWorkersInitialized, 0, 1)
	if !set {
		return errCommitLogReaderMultipleReadloops
	}

	// Start background worker goroutines
	go r.readLoop()
	for _, decoderQueue := range r.decoderQueues {
		localDecoderQueue := decoderQueue
		go r.decoderLoop(localDecoderQueue, r.outChan)
	}

	return nil
}

func (r *reader) readLoop() {
	defer func() {
		for _, decoderQueue := range r.decoderQueues {
			close(decoderQueue)
		}
	}()

	decodingOpts := r.opts.FilesystemOptions().DecodingOptions()
	decoder := msgpack.NewDecoder(decodingOpts)
	decoderStream := msgpack.NewDecoderStream(nil)

	reusedBytes := make([]byte, 0, r.opts.FlushSize())

	for {
		select {
		case <-r.cancelCtx.Done():
			return
		default:
			data, err := r.readChunk(reusedBytes)
			if err != nil {
				if err == io.EOF {
					return
				}

				r.decoderQueues[0] <- decoderArg{
					bytes: nil,
					err:   err,
				}
				continue
			}

			decoderStream.Reset(data)
			decoder.Reset(decoderStream)
			decodeRemainingToken, uniqueIndex, err := decoder.DecodeLogEntryUniqueIndex()

			// Grab a buffer from a pool specific to the decoder loop we're gonna send it to
			shardedIdx := uniqueIndex % uint64(r.numConc)
			bufPool := r.decoderBufPools[shardedIdx]
			buf := <-bufPool

			// Resize the buffer as necessary
			bufCap := len(buf)
			dataLen := len(data)
			if bufCap < dataLen {
				diff := dataLen - bufCap
				for i := 0; i < diff; i++ {
					buf = append(buf, 0)
				}
			}
			buf = buf[:dataLen]

			// Copy into the buffer
			copy(buf, data)

			// Distribute work by the uniqueIndex so that each decoder loop is receiving
			// all datapoints for a given series within relative order.
			r.decoderQueues[shardedIdx] <- decoderArg{
				bytes:                buf,
				err:                  err,
				decodeRemainingToken: decodeRemainingToken,
				uniqueIndex:          uniqueIndex,
				offset:               decoderStream.Offset(),
				bufPool:              bufPool,
			}
		}
	}
}

func (r *reader) decoderLoop(inBuf <-chan decoderArg, outBuf chan<- readResponse) {
	var (
		decodingOpts           = r.opts.FilesystemOptions().DecodingOptions()
		decoder                = msgpack.NewDecoder(decodingOpts)
		decoderStream          = msgpack.NewDecoderStream(nil)
		metadataDecoder        = msgpack.NewDecoder(decodingOpts)
		metadataDecoderStream  = msgpack.NewDecoderStream(nil)
		metadataLookup         = make(map[uint64]seriesMetadata)
		tagDecoder             = r.opts.FilesystemOptions().TagDecoderPool().Get()
		tagDecoderCheckedBytes = checked.NewBytes(nil, nil)
	)
	tagDecoderCheckedBytes.IncRef()
	defer func() {
		tagDecoderCheckedBytes.DecRef()
		tagDecoderCheckedBytes.Finalize()
		tagDecoder.Close()
	}()

	for arg := range inBuf {
		response := readResponse{}
		// If there is a pre-existing error, just pipe it through
		if arg.err != nil {
			r.handleDecoderLoopIterationEnd(arg, outBuf, response, arg.err)
			continue
		}

		// Decode the log entry
		decoderStream.Reset(arg.bytes[arg.offset:])
		decoder.Reset(decoderStream)
		entry, err := decoder.DecodeLogEntryRemaining(arg.decodeRemainingToken, arg.uniqueIndex)
		if err != nil {
			r.handleDecoderLoopIterationEnd(arg, outBuf, response, err)
			continue
		}

		// If the log entry has associated metadata, decode that as well
		if len(entry.Metadata) != 0 {
			err := r.decodeAndHandleMetadata(metadataLookup, metadataDecoder, metadataDecoderStream,
				tagDecoder, tagDecoderCheckedBytes, entry)
			if err != nil {
				r.handleDecoderLoopIterationEnd(arg, outBuf, response, err)
				continue
			}
		}

		metadata, hasMetadata := metadataLookup[entry.Index]
		if !hasMetadata {
			// In this case we know the commit log is corrupt because the commit log writer guarantees
			// that the first entry for a series in the commit log includes its metadata. In addition,
			// even though we are performing parallel decoding, the work is distributed to the workers
			// based on the series unique index which means that all commit log entries for a given
			// series are decoded in-order by a single decoder loop.
			r.handleDecoderLoopIterationEnd(arg, outBuf, response, errCommitLogReaderMissingMetadata)
			continue
		}

		if !metadata.passedPredicate {
			// Pass nil for outBuf because we don't want to send a readResponse along since this
			// was just a series that the caller didn't want us to read.
			r.handleDecoderLoopIterationEnd(arg, nil, readResponse{}, nil)
			continue
		}

		response.series = metadata.Series

		response.datapoint = ts.Datapoint{
			Timestamp: time.Unix(0, entry.Timestamp),
			Value:     entry.Value,
		}
		response.unit = xtime.Unit(byte(entry.Unit))
		// Copy annotation to prevent reference to pooled byte slice
		if len(entry.Annotation) > 0 {
			response.annotation = append([]byte(nil), entry.Annotation...)
		}
		r.handleDecoderLoopIterationEnd(arg, outBuf, response, nil)
	}

	r.metadata.Lock()
	r.metadata.numBlockedOrFinishedDecoders++
	// If all of the decoders are either finished or blocked then we need to free
	// any pending waiters. This also guarantees that the last decoderLoop to
	// finish will free up any pending waiters (and by then any still-pending
	// metadata is definitely missing from the commitlog)
	if r.metadata.numBlockedOrFinishedDecoders >= r.numConc {
		close(outBuf)
	}
	r.metadata.Unlock()
}

func (r *reader) handleDecoderLoopIterationEnd(arg decoderArg, outBuf chan<- readResponse, response readResponse, err error) {
	if arg.bytes != nil {
		arg.bufPool <- arg.bytes
	}
	if outBuf != nil {
		response.resultErr = err
		outBuf <- response
	}
}

func (r *reader) decodeAndHandleMetadata(
	metadataLookup map[uint64]seriesMetadata,
	metadataDecoder *msgpack.Decoder,
	metadataDecoderStream msgpack.DecoderStream,
	tagDecoder serialize.TagDecoder,
	tagDecoderCheckedBytes checked.Bytes,
	entry schema.LogEntry,
) error {
	metadataDecoderStream.Reset(entry.Metadata)
	metadataDecoder.Reset(metadataDecoderStream)
	decoded, err := metadataDecoder.DecodeLogMetadata()
	if err != nil {
		return err
	}

	_, ok := metadataLookup[entry.Index]
	if ok {
		// If the metadata already exists, we can skip this step
		return nil
	}

	id := r.checkedBytesPool.Get(len(decoded.ID))
	id.IncRef()
	id.AppendAll(decoded.ID)

	namespace := r.checkedBytesPool.Get(len(decoded.Namespace))
	namespace.IncRef()
	namespace.AppendAll(decoded.Namespace)

	var (
		tags        ident.Tags
		tagBytesLen = len(decoded.EncodedTags)
	)
	if tagBytesLen != 0 {
		tagDecoderCheckedBytes.Reset(decoded.EncodedTags)
		tagDecoder.Reset(tagDecoderCheckedBytes)

		idPool := r.opts.IdentifierPool()
		tags = idPool.Tags()
		for tagDecoder.Next() {
			curr := tagDecoder.Current()
			tags.Append(idPool.CloneTag(curr))
		}
		err = tagDecoder.Err()
		if err != nil {
			return err
		}
	}

	metadata := ts.Series{
		UniqueIndex: entry.Index,
		ID:          ident.BinaryID(id),
		Namespace:   ident.BinaryID(namespace),
		Shard:       decoded.Shard,
		Tags:        tags,
	}

	metadataLookup[entry.Index] = seriesMetadata{
		Series:          metadata,
		passedPredicate: r.seriesPredicate(metadata.ID, metadata.Namespace),
	}

	namespace.DecRef()
	id.DecRef()
	return nil
}

func (r *reader) readChunk(buf []byte) ([]byte, error) {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return nil, err
	}

	// Extend buffer as necessary
	if len(buf) < int(size) {
		diff := int(size) - len(buf)
		for i := 0; i < diff; i++ {
			buf = append(buf, 0)
		}
	}

	// Size target buffer for reading
	buf = buf[:size]

	// Read message
	if _, err := r.chunkReader.Read(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func (r *reader) readInfo() (schema.LogInfo, error) {
	data, err := r.readChunk([]byte{})
	if err != nil {
		return emptyLogInfo, err
	}
	r.infoDecoderStream.Reset(data)
	r.infoDecoder.Reset(r.infoDecoderStream)
	logInfo, err := r.infoDecoder.DecodeLogInfo()
	return logInfo, err
}

func (r *reader) Close() error {
	// Background goroutines were never started, safe to close immediately.
	if r.nextIndex == 0 {
		return r.close()
	}

	// Shutdown the readLoop goroutine which will shut down the decoderLoops
	// and close the fd
	r.cancelFunc()
	// Drain any unread data from the outBuffers to free any decoderLoops curently
	// in a blocking write
	for {
		_, ok := <-r.outChan
		r.nextIndex++
		if !ok {
			break
		}
	}

	return r.close()
}

func (r *reader) close() error {
	if r.chunkReader.fd == nil {
		return nil
	}

	return r.chunkReader.fd.Close()
}
