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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

const decoderInBufChanSize = 1000
const decoderOutBufChanSize = 1000

var (
	emptyLogInfo schema.LogInfo

	errCommitLogReaderChunkSizeChecksumMismatch     = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderIsNotReusable                 = errors.New("commit log reader is not reusable")
	errCommitLogReaderMultipleReadloops             = errors.New("commitlog reader tried to open multiple readLoops, do not call Read() concurrently")
	errCommitLogReaderPendingResponseCompletedTwice = errors.New("commit log reader pending response was completed twice")
	errCommitLogReaderPendingMetadataNeverFulfilled = errors.New("commit log reader pending metadata was never fulfilled")
)

type readerPendingSeriesMetadataResponse struct {
	wg       sync.WaitGroup
	competed uint32
	value    Series
	err      error
}

func (p *readerPendingSeriesMetadataResponse) done(value Series, err error) error {
	if !atomic.CompareAndSwapUint32(&p.competed, 0, 1) {
		return errCommitLogReaderPendingResponseCompletedTwice
	}
	p.value = value
	p.err = err
	p.wg.Done()
	return nil
}

func (p *readerPendingSeriesMetadataResponse) wait() (Series, error) {
	p.wg.Wait()
	return p.value, p.err
}

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (time.Time, time.Duration, int, error)

	// Read returns the next id and data pair or error, will return io.EOF at end of volume
	Read() (Series, ts.Datapoint, xtime.Unit, ts.Annotation, uint64, error)

	// Close the reader
	Close() error
}

type readResponse struct {
	series      Series
	datapoint   ts.Datapoint
	unit        xtime.Unit
	annotation  ts.Annotation
	uniqueIndex uint64
	resultErr   error
}

type decoderArg struct {
	bytes  []byte
	err    error
	index  uint64
	offset int
}

type readerMetadata struct {
	sync.RWMutex
	numBlockedOrFinishedDecoders int64
}

type reader struct {
	opts                 Options
	numConc              int64
	checkedBytesPool     pool.CheckedBytesPool
	bytesPool            pool.BytesPool
	chunkReader          *chunkReader
	dataBuffer           []byte
	infoDecoder          *msgpack.Decoder
	infoDecoderStream    msgpack.DecoderStream
	decoderBufs          []chan decoderArg
	outBuf               chan readResponse
	cancelCtx            context.Context
	cancelFunc           context.CancelFunc
	shutdownCh           chan error
	metadata             readerMetadata
	nextIndex            int64
	hasBeenOpened        bool
	bgWorkersInitialized int64
	yolo                 []byte
}

func newCommitLogReader(opts Options) commitLogReader {
	decodingOpts := opts.FilesystemOptions().DecodingOptions()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	numConc := opts.ReadConcurrency()
	decoderBufs := make([]chan decoderArg, 0, numConc)
	for i := 0; i < numConc; i++ {
		decoderBufs = append(decoderBufs, make(chan decoderArg, decoderInBufChanSize))
	}
	outBuf := make(chan readResponse, decoderOutBufChanSize*numConc)

	reader := &reader{
		opts:              opts,
		numConc:           int64(numConc),
		checkedBytesPool:  opts.BytesPool(),
		chunkReader:       newChunkReader(opts.FlushSize()),
		infoDecoder:       msgpack.NewDecoder(decodingOpts),
		infoDecoderStream: msgpack.NewDecoderStream(nil),
		decoderBufs:       decoderBufs,
		outBuf:            outBuf,
		cancelCtx:         cancelCtx,
		cancelFunc:        cancelFunc,
		shutdownCh:        make(chan error),
		metadata:          readerMetadata{},
		nextIndex:         0,
		yolo:              []byte("metricslol"),
	}
	return reader
}

func (r *reader) Open(filePath string) (time.Time, time.Duration, int, error) {
	// Commitlog reader does not currently support being reused
	if r.hasBeenOpened {
		return timeZero, 0, 0, errCommitLogReaderIsNotReusable
	}
	r.hasBeenOpened = true

	fd, err := os.Open(filePath)
	if err != nil {
		return timeZero, 0, 0, err
	}

	r.chunkReader.reset(fd)
	info, err := r.readInfo()
	if err != nil {
		r.Close()
		return timeZero, 0, 0, err
	}
	start := time.Unix(0, info.Start)
	duration := time.Duration(info.Duration)
	index := int(info.Index)

	return start, duration, index, nil
}

func (r *reader) Read() (
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
	uniqueIndex uint64,
	resultErr error,
) {
	if r.nextIndex == 0 {
		err := r.startBackgroundWorkers()
		if err != nil {
			return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), 0, err
		}
	}
	rr, ok := <-r.outBuf
	if !ok {
		return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), 0, io.EOF
	}
	r.nextIndex++
	return rr.series, rr.datapoint, rr.unit, rr.annotation, rr.uniqueIndex, rr.resultErr
}

func (r *reader) startBackgroundWorkers() error {
	// Make sure background workers are never setup more than once
	set := atomic.CompareAndSwapInt64(&r.bgWorkersInitialized, 0, 1)
	if !set {
		return errCommitLogReaderMultipleReadloops
	}

	// Start background worker goroutines
	go r.readLoop()
	for _, decoderBuf := range r.decoderBufs {
		localDecoderBuf := decoderBuf
		go r.decoderLoop(localDecoderBuf, r.outBuf)
	}

	return nil
}

func (r *reader) readLoop() {
	defer r.shutdown()

	decodingOpts := r.opts.FilesystemOptions().DecodingOptions()
	decoder := msgpack.NewDecoder(decodingOpts)
	decoderStream := msgpack.NewDecoderStream(nil)
	for {
		select {
		case <-r.cancelCtx.Done():
			return
		default:
			data, err := r.readChunk()
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(err)
			}

			decoderStream.Reset(data)
			decoder.Reset(decoderStream)
			_, index, err := decoder.DecodeLogEntryPart1()
			if err != nil {
				panic(err)
			}

			// Distribute the decoding work in round-robin fashion so that when we
			// read round-robin, we get the data back in the original order.
			r.decoderBufs[index%uint64(r.numConc)] <- decoderArg{
				bytes:  data,
				err:    err,
				index:  index,
				offset: decoderStream.Offset(),
			}
		}
	}
}

func (r *reader) shutdown() {
	for _, decoderBuf := range r.decoderBufs {
		close(decoderBuf)
	}
	r.shutdownCh <- r.close()
}

func (r *reader) decoderLoop(inBuf <-chan decoderArg, outBuf chan<- readResponse) {
	var (
		decodingOpts          = r.opts.FilesystemOptions().DecodingOptions()
		decoder               = msgpack.NewDecoder(decodingOpts)
		decoderStream         = msgpack.NewDecoderStream(nil)
		metadataDecoder       = msgpack.NewDecoder(decodingOpts)
		metadataDecoderStream = msgpack.NewDecoderStream(nil)
		metadataLookup        = make(map[uint64]Series)
	)

	for arg := range inBuf {
		readResponse := readResponse{}
		// If there is a pre-existing error, just pipe it through
		if arg.err != nil {
			readResponse.resultErr = arg.err
			outBuf <- readResponse
			continue
		}

		// Decode the log entry
		decoderStream.Reset(arg.bytes[arg.offset:])
		decoder.Reset(decoderStream)
		entry, err := decoder.DecodeLogEntryPart2()
		if err != nil {
			readResponse.resultErr = err
			outBuf <- readResponse
			continue
		}
		entry.Index = arg.index

		// If the log entry has associated metadata, decode that as well
		if len(entry.Metadata) != 0 {
			err := r.decodeAndHandleMetadata(metadataLookup, metadataDecoder, metadataDecoderStream, entry)
			if err != nil {
				readResponse.resultErr = err
				outBuf <- readResponse
				continue
			}
		}

		metadata, hasMetadata := metadataLookup[entry.Index]
		// The required metadata hasn't been processed yet
		if !hasMetadata {
			panic("MISSING METADATA")
		}
		readResponse.series = metadata

		readResponse.datapoint = ts.Datapoint{
			Timestamp: time.Unix(0, entry.Timestamp),
			Value:     entry.Value,
		}
		readResponse.unit = xtime.Unit(byte(entry.Unit))
		readResponse.uniqueIndex = entry.Index
		// Copy annotation to prevent reference to pooled byte slice
		if len(entry.Annotation) > 0 {
			readResponse.annotation = append([]byte(nil), entry.Annotation...)
		}
		outBuf <- readResponse
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

func (r *reader) decodeAndHandleMetadata(
	metadataLookup map[uint64]Series,
	metadataDecoder *msgpack.Decoder,
	metadataDecoderStream msgpack.DecoderStream,
	entry schema.LogEntry,
) error {
	metadataDecoderStream.Reset(entry.Metadata)
	metadataDecoder.Reset(metadataDecoderStream)
	decoded, err := metadataDecoder.DecodeLogMetadata()
	if err != nil {
		return err
	}

	id := r.checkedBytesPool.Get(len(decoded.ID))
	id.IncRef()
	id.AppendAll(decoded.ID)

	namespace := r.checkedBytesPool.Get(len(decoded.Namespace))
	namespace.IncRef()
	namespace.AppendAll(decoded.Namespace)

	if bytes.Equal(namespace.Get(), r.yolo) {
		panic("Wtf")
	}

	_, ok := metadataLookup[entry.Index]
	// If the metadata already exists, we can skip this step
	if ok {
		id.DecRef()
		id.Finalize()
		namespace.DecRef()
		namespace.Finalize()
	} else {
		metadata := Series{
			UniqueIndex: entry.Index,
			ID:          ident.BinaryID(id),
			Namespace:   ident.BinaryID(namespace),
			Shard:       decoded.Shard,
		}
		metadataLookup[entry.Index] = metadata

		namespace.DecRef()
		id.DecRef()
	}
	return nil
}

func (r *reader) lookupMetadata(metadataLookup map[uint64]Series, entryIndex uint64) Series {
	metadata, hasMetadata := metadataLookup[entryIndex]

	// The required metadata hasn't been processed yet
	if !hasMetadata {
		panic("MISSING METADATA")
	}

	return metadata
}

func (r *reader) readChunk() ([]byte, error) {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return nil, err
	}

	// TODO: Consider pooling this
	b := make([]byte, int(size))
	// Read message
	if _, err := r.chunkReader.Read(b); err != nil {
		return nil, err
	}

	return b, nil
}

func (r *reader) readInfo() (schema.LogInfo, error) {
	data, err := r.readChunk()
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
		_, ok := <-r.outBuf
		r.nextIndex++
		if !ok {
			break
		}
	}
	shutdownErr := <-r.shutdownCh
	return shutdownErr
}

func (r *reader) close() error {
	if r.chunkReader.fd == nil {
		return nil
	}
	return r.chunkReader.fd.Close()
}
