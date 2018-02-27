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
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

// decoderInBufChanSize should be less than 20% of decoderOutBufChanSize
const decoderInBufChanSize = 100
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
	pendingMetadata *readerPendingSeriesMetadataResponse
	series          Series
	datapoint       ts.Datapoint
	unit            xtime.Unit
	annotation      ts.Annotation
	uniqueIndex     uint64
	resultErr       error
}

type decoderArg struct {
	bytes checked.Bytes
	err   error
}

type lockableMetadataLookup struct {
	*sync.RWMutex
	lookup  map[uint64]Series
	pending map[uint64][]*readerPendingSeriesMetadataResponse
}

type readerMetadata struct {
	sync.RWMutex
	numBlockedOrFinishedDecoders int64
	shardedLookup                []lockableMetadataLookup
}

type reader struct {
	opts                 Options
	numConc              int64
	bytesPool            pool.CheckedBytesPool
	chunkReader          *chunkReader
	dataBuffer           []byte
	infoDecoder          *msgpack.Decoder
	infoDecoderStream    msgpack.DecoderStream
	decoderBufs          []chan decoderArg
	outBufs              []chan readResponse
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
	outBufs := make([]chan readResponse, 0, numConc)
	for i := 0; i < numConc; i++ {
		outBufs = append(outBufs, make(chan readResponse, decoderOutBufChanSize))
	}

	shardedLookup := make([]lockableMetadataLookup, 0, 1000)
	for i := 0; i < numConc; i++ {
		shardedLookup = append(shardedLookup, lockableMetadataLookup{
			RWMutex: &sync.RWMutex{},
			lookup:  make(map[uint64]Series),
			pending: make(map[uint64][]*readerPendingSeriesMetadataResponse),
		})
	}

	reader := &reader{
		opts:              opts,
		numConc:           int64(numConc),
		bytesPool:         opts.BytesPool(),
		chunkReader:       newChunkReader(opts.FlushSize()),
		infoDecoder:       msgpack.NewDecoder(decodingOpts),
		infoDecoderStream: msgpack.NewDecoderStream(nil),
		decoderBufs:       decoderBufs,
		outBufs:           outBufs,
		cancelCtx:         cancelCtx,
		cancelFunc:        cancelFunc,
		shutdownCh:        make(chan error),
		metadata: readerMetadata{
			shardedLookup: shardedLookup,
		},
		nextIndex: 0,
		yolo:      []byte("metricslol"),
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
	nextIndex := atomic.LoadInt64(&r.nextIndex)
	if nextIndex == 0 {
		err := r.startBackgroundWorkers()
		if err != nil {
			return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), 0, err
		}
	}
	// Data is written into these channels in round-robin fashion, so reading them
	// one at a time in the same order results in an ordered stream. I.E with a
	// concurrency of two then we'll start with an initial structure like this:
	// 		r.outBufs:
	// 			[0]: []
	// 			[1]: []
	// After several reads from the commitlog we end up with:
	// 		r.outBufs:
	// 			[0]: [a, c]
	// 			[1]: [b, d]
	// If we now read from the outBufs in round-robin order (0, 1, 0, 1) we will
	// end up with the correct global ordering: a, b, c, d
	rr, ok := <-r.outBufs[r.nextIndex%r.numConc]
	if !ok {
		return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), 0, io.EOF
	}
	if rr.pendingMetadata != nil {
		// Wait for metadata to be available
		rr.series, rr.resultErr = rr.pendingMetadata.wait()
	}
	atomic.StoreInt64(&r.nextIndex, nextIndex+1)
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
	for i, decoderBuf := range r.decoderBufs {
		localDecoderBuf, outBuf := decoderBuf, r.outBufs[i]
		go r.decoderLoop(localDecoderBuf, outBuf)
	}

	return nil
}

func (r *reader) readLoop() {
	index := int64(0)
	defer r.shutdown()
	for {
		numRead := atomic.LoadInt64(&r.nextIndex)
		numPending := index - numRead
		numPendingPerBuf := numPending / r.numConc
		if numPendingPerBuf > 0.8*decoderOutBufChanSize {
			// If the number of datapoints that have been read is less than 80% of the
			// datapoints that have been pushed downstream, start backing off to accomodate
			// a slow reader and prevent the decoderOutBufs from all filling up.
			time.Sleep(time.Millisecond)
			continue
		}
		select {
		case <-r.cancelCtx.Done():
			return
		default:
			data, err := r.readChunk()
			// Distribute the decoding work in round-robin fashion so that when we
			// read round-robin, we get the data back in the original order.
			r.decoderBufs[index%r.numConc] <- decoderArg{
				bytes: data,
				err:   err,
			}
			index++
			if err == io.EOF {
				return
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
	)
	for arg := range inBuf {
		readResponse := readResponse{}
		// If there is a pre-existing error, just pipe it through
		if arg.err != nil {
			readResponse.resultErr = arg.err
			// outBuf <- readResponse
			r.writeToOutBuf(outBuf, readResponse)
			continue
		}

		// Decode the log entry
		// arg.bytes.IncRef()
		decoderStream.Reset(arg.bytes.Get())
		decoder.Reset(decoderStream)
		entry, err := decoder.DecodeLogEntry()
		if err != nil {
			readResponse.resultErr = err
			// outBuf <- readResponse
			r.writeToOutBuf(outBuf, readResponse)
			continue
		}

		// If the log entry has associated metadata, decode that as well
		if len(entry.Metadata) != 0 {
			err := r.decodeAndHandleMetadata(metadataDecoder, metadataDecoderStream, entry)
			if err != nil {
				readResponse.resultErr = err
				// outBuf <- readResponse
				r.writeToOutBuf(outBuf, readResponse)
				continue
			}
		}

		// Metadata may or may not actually be nonzero here, the consumer in Read()
		// will wait if so
		readResponse.series, readResponse.pendingMetadata = r.lookupMetadata(entry.Index)
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
		// DecRef delayed until this point because even after decoding, underlying
		// byte slice is still referenced by metadata and annotation
		arg.bytes.DecRef()
		arg.bytes.Finalize()
		// outBuf <- readResponse
		r.writeToOutBuf(outBuf, readResponse)
	}

	r.metadata.Lock()
	r.metadata.numBlockedOrFinishedDecoders++
	// If all of the decoders are either finished or blocked then we need to free
	// any pending waiters. This also guarantees that the last decoderLoop to
	// finish will free up any pending waiters (and by then any still-pending
	// metadata is definitely missing from the commitlog)
	if r.metadata.numBlockedOrFinishedDecoders >= r.numConc {
		r.freeAllPendingWaitersWithLock()
	}
	r.metadata.Unlock()
	// Close the outBuf now that there is no more data to produce to it
	close(outBuf)
}

func (r *reader) decodeAndHandleMetadata(
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

	id := r.bytesPool.Get(len(decoded.ID))
	id.IncRef()
	id.AppendAll(decoded.ID)

	namespace := r.bytesPool.Get(len(decoded.Namespace))
	namespace.IncRef()
	namespace.AppendAll(decoded.Namespace)

	if bytes.Equal(namespace.Get(), r.yolo) {
		panic("Wtf")
	}

	lookup := r.metadata.shardedLookup[entry.Index%uint64(r.numConc)]
	lookup.Lock()
	_, ok := lookup.lookup[entry.Index]
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
		lookup.lookup[entry.Index] = metadata
		pendingMetadata, ok := lookup.pending[entry.Index]

		namespace.DecRef()
		id.DecRef()

		// One (or more) goroutines are blocked waiting for this metadata, release them
		if ok {
			delete(lookup.pending, entry.Index)
			for _, p := range pendingMetadata {
				p.done(metadata, nil)
			}
		}
	}
	lookup.Unlock()
	return nil
}

func (r *reader) lookupMetadata(entryIndex uint64) (
	metadata Series,
	pendingMetadata *readerPendingSeriesMetadataResponse,
) {
	lookup := r.metadata.shardedLookup[entryIndex%uint64(r.numConc)]
	// Try cheap read lock first
	lookup.RLock()
	metadata, hasMetadata := lookup.lookup[entryIndex]
	lookup.RUnlock()

	// The required metadata hasn't been processed yet
	if !hasMetadata {
		lookup.Lock()
		metadata, hasMetadata = lookup.lookup[entryIndex]
		if !hasMetadata {
			pendingMetadata = &readerPendingSeriesMetadataResponse{}
			pendingMetadata.wg.Add(1)
			lookup.pending[entryIndex] = append(lookup.pending[entryIndex], pendingMetadata)
		}
		lookup.Unlock()
	}

	return metadata, pendingMetadata
}

// TODO: Might not need this function anymore
func (r *reader) writeToOutBuf(outBuf chan<- readResponse, readResponse readResponse) {
	// Guard against all of the decoder loops having full outBuffers (deadlock).
	// This could happen in the scenario where the commitlog is missing metadata
	// and the .Wait() call in Read() is blocked waiting for data that will
	// never arrive and thus is unable to empty the outBufs.
	// It can also occur in the case that the reader is unable to keep up with the
	// background workers, in which case the outBufs can also fill up. We guard against
	// the latter case by implementing back-off in the readLoop.
	for {
		select {
		// Happy path, outBuf is not full and our write succeeds immediately
		case outBuf <- readResponse:
			return
		// outBuf is full - fallthrough into remaining code
		default:
		}

		r.metadata.Lock()
		// Check if all the other decoderLoops are finished or blocked too
		notAllBlockedOrFinished := r.metadata.numBlockedOrFinishedDecoders+1 < r.numConc
		// If they're not then register ourselves as blocked and perform a blocking write
		if notAllBlockedOrFinished {
			r.metadata.numBlockedOrFinishedDecoders++
			// Release the lock because we're about to perform a potentially blocking
			// channel write
			r.metadata.Unlock()

			// Safe to perform a blocking write because we will be free'd by the
			// final decoderLoop if its about to block or finish
			outBuf <- readResponse

			// Unregister as blocked
			r.metadata.Lock()
			r.metadata.numBlockedOrFinishedDecoders--
			r.metadata.Unlock()
			return
		}

		// If all the other workers are blocked or finished, then performing a
		// blocking write would cause deadlock so we free all pending waiters
		r.freeAllPendingWaitersWithLock()
		r.metadata.Unlock()
	}
}

func (r *reader) freeAllPendingWaitersWithLock() {
	for _, shardedLookup := range r.metadata.shardedLookup {
		for _, pending := range shardedLookup.pending {
			for _, p := range pending {
				p.done(Series{}, errCommitLogReaderPendingMetadataNeverFulfilled)
			}
		}
		for k := range shardedLookup.pending {
			delete(shardedLookup.pending, k)
		}
	}
}

func (r *reader) readChunk() (checked.Bytes, error) {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return nil, err
	}

	// Extend buffer as necessary
	// if len(r.dataBuffer) < int(size) {
	// 	diff := int(size) - len(r.dataBuffer)
	// 	for i := 0; i < diff; i++ {
	// 		r.dataBuffer = append(r.dataBuffer, 0)
	// 	}
	// }

	// Size the target buffer for reading and unmarshalling
	// buffer := r.dataBuffer[:size]
	checkedBytes := r.bytesPool.Get(int(size))
	checkedBytes.IncRef()
	checkedBytes.Resize(int(size))

	// Read message

	if _, err := r.chunkReader.Read(checkedBytes.Get()); err != nil {
		// checkedBytes.DecRef()
		return nil, err
	}
	// checkedBytes.DecRef()

	// Copy message into pooled byte slice. Reading happens synchronously so its
	// safe to reuse the same buffer repeatedly, but decoding happens in parallel
	// so we need to make sure that the byte slice we return won't be mutated
	// until the caller is done with it.
	// pooled := r.bytesPool.Get(int(size))
	// pooled.IncRef()
	// pooled.AppendAll(buffer)
	// pooled.DecRef()
	return checkedBytes, nil
}

func (r *reader) readInfo() (schema.LogInfo, error) {
	data, err := r.readChunk()
	if err != nil {
		return emptyLogInfo, err
	}
	data.IncRef()
	r.infoDecoderStream.Reset(data.Get())
	r.infoDecoder.Reset(r.infoDecoderStream)
	logInfo, err := r.infoDecoder.DecodeLogInfo()
	data.DecRef()
	data.Finalize()
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
		_, ok := <-r.outBufs[r.nextIndex%r.numConc]
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
