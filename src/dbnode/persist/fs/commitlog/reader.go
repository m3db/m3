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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3db/src/dbnode/persist/schema"
	"github.com/m3db/m3db/src/dbnode/serialize"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

const defaultDecodeEntryBufSize = 1024
const decoderInBufChanSize = 1000
const decoderOutBufChanSize = 1000

var (
	emptyLogInfo schema.LogInfo

	errCommitLogReaderChunkSizeChecksumMismatch = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderMissingMetadata           = errors.New("commit log reader encountered a datapoint without corresponding metadata")
	errCommitLogReaderIsAlreadyClosed           = errors.New("commit log reader is already closed")
	errCommitLogReaderIsAlreadyOpen             = errors.New("commit log reader is already open")
	errBackgroundWorkersAlreadyInitialized      = fmt.Errorf(
		"%s commit log reader background workers already initialized", instrument.InvariantViolatedMetricName)
)

// ReadAllSeriesPredicate can be passed as the seriesPredicate for callers
// that want a convenient way to read all series in the commitlogs
func ReadAllSeriesPredicate() SeriesFilterPredicate {
	return func(id ident.ID, namespace ident.ID) bool { return true }
}

type seriesMetadata struct {
	Series
	passedPredicate bool
}

type commitLogReader interface {
	// Open opens the commit log for reading
	Open(filePath string) (time.Time, time.Duration, int, error)

	// Read returns the next id and data pair or error, will return io.EOF at end of volume
	Read() (Series, ts.Datapoint, xtime.Unit, ts.Annotation, error)

	// Close the reader
	Close() error
}

type readResponse struct {
	series     Series
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

type completedDecoders struct {
	sync.RWMutex
	numCompletedDecoders int64
	wg                   *sync.WaitGroup
}

type reader struct {
	// The commit log reader is not goroutine safe, but we use an embedded
	// mutex to defensively protect the Open/Close methods against misuse.
	// We don't protect the Read() method with the mutex because it would
	// be too expensive.
	sync.Mutex
	opts                 Options
	numConc              int64
	checkedBytesPool     pool.CheckedBytesPool
	chunkReader          *chunkReader
	infoDecoder          *msgpack.Decoder
	infoDecoderStream    msgpack.DecoderStream
	outBuf               chan readResponse
	doneCh               chan struct{}
	completedDecoders    completedDecoders
	nextIndex            int64
	bgWorkersInitialized bool
	seriesPredicate      SeriesFilterPredicate
	isOpen               bool
}

func newCommitLogReader(opts Options, seriesPredicate SeriesFilterPredicate) commitLogReader {
	var (
		decodingOpts = opts.FilesystemOptions().DecodingOptions()
		numConc      = opts.ReadConcurrency()
	)

	reader := &reader{
		opts:              opts,
		numConc:           int64(numConc),
		checkedBytesPool:  opts.BytesPool(),
		chunkReader:       newChunkReader(opts.FlushSize()),
		infoDecoder:       msgpack.NewDecoder(decodingOpts),
		infoDecoderStream: msgpack.NewDecoderStream(nil),
		completedDecoders: completedDecoders{wg: &sync.WaitGroup{}},
		nextIndex:         0,
		seriesPredicate:   seriesPredicate,
	}
	reader.reset()

	return reader
}

func (r *reader) Open(filePath string) (time.Time, time.Duration, int, error) {
	r.Lock()
	defer r.Unlock()

	if r.isOpen {
		return timeZero, 0, 0, errCommitLogReaderIsAlreadyOpen
	}

	r.reset()
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

	r.isOpen = true
	return start, duration, index, nil
}

// Read guarantees that the datapoints it returns will be in the same order as they are on disk
// for a given series, but they will not be in the same order they are on disk across series.
// I.E, if the commit log looked like this (letters are series and numbers are writes):
// A1, B1, B2, A2, C1, D1, D2, A3, B3, D2
// Then the caller is guaranteed to receive A1 before A2 and A2 before A3, and they are guaranteed
// to see B1 before B2, but they may see B1 before A1 and D2 before B3.
func (r *reader) Read() (
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
	resultErr error,
) {
	if r.nextIndex == 0 {
		r.startBackgroundWorkers()
	}
	rr, ok := <-r.outBuf
	if !ok {
		return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), io.EOF
	}
	r.nextIndex++
	return rr.series, rr.datapoint, rr.unit, rr.annotation, rr.resultErr
}

func (r *reader) startBackgroundWorkers() error {
	if r.bgWorkersInitialized {
		// Should never happen
		return errBackgroundWorkersAlreadyInitialized
	}

	var (
		numConc       = r.opts.ReadConcurrency()
		decoderQueues = make([]chan decoderArg, 0, numConc)
	)
	for i := 0; i < numConc; i++ {
		decoderQueues = append(decoderQueues, make(chan decoderArg, decoderInBufChanSize))
	}

	go r.readLoop(decoderQueues)
	for _, decoderQueue := range decoderQueues {
		localDecoderQueue := decoderQueue
		r.completedDecoders.wg.Add(1)
		go r.decoderLoop(localDecoderQueue)
	}

	return nil
}

func (r *reader) readLoop(decoderQueues []chan decoderArg) {
	defer func() {
		for _, decoderQueue := range decoderQueues {
			close(decoderQueue)
		}
	}()

	var (
		decodingOpts    = r.opts.FilesystemOptions().DecodingOptions()
		decoder         = msgpack.NewDecoder(decodingOpts)
		decoderStream   = msgpack.NewDecoderStream(nil)
		reusedBytes     = make([]byte, 0, r.opts.FlushSize())
		numConc         = r.opts.ReadConcurrency()
		decoderBufPools = make([]chan []byte, 0, numConc)
	)

	for i := 0; i < numConc; i++ {
		chanBufs := make(chan []byte, decoderInBufChanSize+1)
		for i := 0; i < decoderInBufChanSize+1; i++ {
			// Bufs will be resized as needed, so its ok if our default isn't big enough
			chanBufs <- make([]byte, defaultDecodeEntryBufSize)
		}
		decoderBufPools = append(decoderBufPools, chanBufs)
	}

	for {
		select {
		case <-r.doneCh:
			return
		default:
			data, err := r.readChunk(reusedBytes)
			if err != nil {
				if err == io.EOF {
					return
				}
				decoderQueues[0] <- decoderArg{
					bytes: data,
					err:   err,
				}
				continue
			}

			decoderStream.Reset(data)
			decoder.Reset(decoderStream)
			decodeRemainingToken, uniqueIndex, err := decoder.DecodeLogEntryUniqueIndex()

			// Grab a buffer from a pool specific to the decoder loop we're gonna send it to
			shardedIdx := uniqueIndex % uint64(r.numConc)
			bufPool := decoderBufPools[shardedIdx]
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
			decoderQueues[shardedIdx] <- decoderArg{
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

func (r *reader) decoderLoop(inBuf <-chan decoderArg) {
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
			r.handleDecoderLoopIterationEnd(arg, true, response, arg.err)
			continue
		}

		// Decode the log entry
		decoderStream.Reset(arg.bytes[arg.offset:])
		decoder.Reset(decoderStream)
		entry, err := decoder.DecodeLogEntryRemaining(arg.decodeRemainingToken, arg.uniqueIndex)
		if err != nil {
			r.handleDecoderLoopIterationEnd(arg, true, response, err)
			continue
		}

		// If the log entry has associated metadata, decode that as well
		if len(entry.Metadata) != 0 {
			err := r.decodeAndHandleMetadata(metadataLookup, metadataDecoder, metadataDecoderStream,
				tagDecoder, tagDecoderCheckedBytes, entry)
			if err != nil {
				r.handleDecoderLoopIterationEnd(arg, true, response, err)
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
			r.handleDecoderLoopIterationEnd(arg, true, response, errCommitLogReaderMissingMetadata)
			continue
		}

		if !metadata.passedPredicate {
			// Pass nil for outBuf because we don't want to send a readResponse along since this
			// was just a series that the caller didn't want us to read.
			r.handleDecoderLoopIterationEnd(arg, false, readResponse{}, nil)
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
		r.handleDecoderLoopIterationEnd(arg, true, response, nil)
	}

	r.completedDecoders.Lock()
	r.completedDecoders.numCompletedDecoders++
	// Once all the decoder are done, we need to close the outBuf so that
	// the final call to Read() won't block forever trying to read out of
	// outBuf.
	if r.completedDecoders.numCompletedDecoders >= r.numConc {
		close(r.outBuf)
	}
	r.completedDecoders.Unlock()
	r.completedDecoders.wg.Done()
}

func (r *reader) handleDecoderLoopIterationEnd(arg decoderArg, shouldSend bool, response readResponse, err error) {
	arg.bufPool <- arg.bytes
	if shouldSend {
		response.resultErr = err
		r.outBuf <- response
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

	metadata := Series{
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

func (r *reader) reset() {
	r.chunkReader.reset(nil)
	r.infoDecoder.Reset(nil)
	r.infoDecoderStream.Reset(nil)
	r.infoDecoder.Reset(nil)
	r.infoDecoderStream.Reset(nil)

	// Shouldn't need the lock here at all, but take it
	// just to be safe.
	r.completedDecoders.Lock()
	r.completedDecoders.numCompletedDecoders = 0
	r.completedDecoders.Unlock()

	r.nextIndex = 0
	r.doneCh = make(chan struct{})
	r.outBuf = make(chan readResponse, decoderOutBufChanSize*r.opts.ReadConcurrency())
}

func (r *reader) Close() error {
	r.Lock()
	defer r.Unlock()

	if !r.isOpen {
		return errCommitLogReaderIsAlreadyClosed
	}

	// Shutdown the readLoop goroutine which will shut down the decoderLoops
	close(r.doneCh)

	// Drain any unread data from the outBuffers to free any decoderLoops curently
	// in a blocking write
	for {
		_, ok := <-r.outBuf
		r.nextIndex++
		if !ok {
			break
		}
	}

	// Wait for all the decoder loops to shutdown.
	r.completedDecoders.wg.Wait()
	return r.close()
}

func (r *reader) close() error {
	if r.chunkReader.fd == nil {
		return nil
	}

	r.isOpen = false
	return r.chunkReader.fd.Close()
}
