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
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

const decoderInBufChanSize = 100

var (
	emptyLogInfo schema.LogInfo

	errCommitLogReaderChunkSizeChecksumMismatch = errors.New("commit log reader encountered chunk size checksum mismatch")
	errCommitLogReaderMissingLogMetadata        = errors.New("commit log reader encountered message missing metadata")
	errCommitLogReaderIsNotReusable             = errors.New("commit log reader is not reusable")
)

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
	bytes checked.Bytes
	err   error
}

type readerMetadata struct {
	sync.RWMutex
	lookup map[uint64]Series
	wgs    map[uint64]*sync.WaitGroup
}

type reader struct {
	opts          Options
	numConc       int
	bytesPool     pool.CheckedBytesPool
	chunkReader   *chunkReader
	dataBuffer    []byte
	logDecoder    encoding.Decoder
	decoderBufs   []chan decoderArg
	outBufs       []chan readResponse
	cancelCtx     context.Context
	cancelFunc    context.CancelFunc
	shutdownCh    chan error
	metadata      readerMetadata
	nextIndex     int
	hasBeenOpened bool
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
		outBufs = append(outBufs, make(chan readResponse, decoderInBufChanSize))
	}

	reader := &reader{
		opts:        opts,
		numConc:     numConc,
		bytesPool:   opts.BytesPool(),
		chunkReader: newChunkReader(opts.FlushSize()),
		logDecoder:  msgpack.NewDecoder(decodingOpts),
		decoderBufs: decoderBufs,
		outBufs:     outBufs,
		cancelCtx:   cancelCtx,
		cancelFunc:  cancelFunc,
		shutdownCh:  make(chan error),
		metadata: readerMetadata{
			lookup: make(map[uint64]Series),
			wgs:    make(map[uint64]*sync.WaitGroup),
		},
		nextIndex: 0,
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
	resultErr error,
) {
	if r.nextIndex == 0 {
		go r.readLoop()
		for i, decoderBuf := range r.decoderBufs {
			go r.decoderLoop(decoderBuf, r.outBufs[i])
		}
	}
	// Data is written into these channels in round-robin fashion, so reading them
	// one at a time in the same order results in an ordered stream.
	rr, ok := <-r.outBufs[r.nextIndex%r.numConc]
	if !ok {
		return Series{}, ts.Datapoint{}, xtime.Unit(0), ts.Annotation(nil), io.EOF
	}
	r.nextIndex++
	return rr.series, rr.datapoint, rr.unit, rr.annotation, rr.resultErr
}

func (r *reader) readLoop() {
	index := 0
	eofFound := false
	for {
		// Return's after calls to r.shutdown() are important otherwise we'll try
		// and close the same channel twice and panic
		select {
		case <-r.cancelCtx.Done():
			r.shutdown()
			return
		default:
			if eofFound {
				r.shutdown()
				return
			}
			data, err := r.readChunk()
			if err == io.EOF {
				eofFound = true
			}

			// Distribute the decoding work in round-robin fashion so that when we
			// read round-robin, we get the data back in the original order.
			r.decoderBufs[index%r.numConc] <- decoderArg{
				bytes: data,
				err:   err,
			}

			index++
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
	decodingOpts := r.opts.FilesystemOptions().DecodingOptions()
	decoder := msgpack.NewDecoder(decodingOpts)
	metadataDecoder := msgpack.NewDecoder(decodingOpts)

	for arg := range inBuf {
		readResponse := readResponse{}
		if arg.err != nil {
			readResponse.resultErr = arg.err
			outBuf <- readResponse
			continue
		}

		arg.bytes.IncRef()
		decoder.Reset(arg.bytes.Get())
		entry, err := decoder.DecodeLogEntry()
		if err != nil {
			readResponse.resultErr = err
			outBuf <- readResponse
			continue
		}

		if len(entry.Metadata) != 0 {
			metadataDecoder.Reset(entry.Metadata)
			decoded, err := metadataDecoder.DecodeLogMetadata()
			if err != nil {
				readResponse.resultErr = err
				outBuf <- readResponse
				continue
			}

			id := r.bytesPool.Get(len(decoded.ID))
			id.IncRef()
			id.AppendAll(decoded.ID)

			namespace := r.bytesPool.Get(len(decoded.Namespace))
			namespace.IncRef()
			namespace.AppendAll(decoded.Namespace)

			r.metadata.Lock()
			r.metadata.lookup[entry.Index] = Series{
				UniqueIndex: entry.Index,
				ID:          ts.BinaryID(id),
				Namespace:   ts.BinaryID(namespace),
				Shard:       decoded.Shard,
			}
			waitGroup, ok := r.metadata.wgs[entry.Index]
			// Another goroutine is blocked waiting for this metadata
			if ok {
				waitGroup.Done()
			}
			r.metadata.Unlock()
			namespace.DecRef()
			id.DecRef()
		}

		var pendingResult *sync.WaitGroup
		var metadata Series
		var hasMetadata bool
		var ok bool

		// Try cheap read lock first
		r.metadata.RLock()
		metadata, hasMetadata = r.metadata.lookup[entry.Index]
		r.metadata.RUnlock()

		// The required metadata hasn't been processed yet
		if !hasMetadata {
			r.metadata.Lock()
			metadata, hasMetadata = r.metadata.lookup[entry.Index]
			if !hasMetadata {
				waitGroup, hasWaitGroup := r.metadata.wgs[entry.Index]
				if hasWaitGroup {
					pendingResult = waitGroup
				} else {
					pendingResult = &sync.WaitGroup{}
					pendingResult.Add(1)
					r.metadata.wgs[entry.Index] = pendingResult
				}
			}
			r.metadata.Unlock()
		}

		// The required metadata hasn't been processed yet
		if pendingResult != nil {
			pendingResult.Wait()
			r.metadata.RLock()
			metadata, ok = r.metadata.lookup[entry.Index]
			r.metadata.RUnlock()
			// Something went wrong, maybe the reader was closed early
			if !ok {
				readResponse.resultErr = errCommitLogReaderMissingLogMetadata
				outBuf <- readResponse
				continue
			}
		}

		readResponse.series = metadata
		readResponse.datapoint = ts.Datapoint{
			Timestamp: time.Unix(0, entry.Timestamp),
			Value:     entry.Value,
		}
		readResponse.unit = xtime.Unit(byte(entry.Unit))
		// Copy annotation to prevent reference to pooled byte slice
		if len(entry.Annotation) > 0 {
			readResponse.annotation = append([]byte(nil), entry.Annotation...)
		}
		// DecRef delayed until this point because even after decoding, underlying
		// byte slice is still referenced by metadata and annotation
		arg.bytes.DecRef()
		arg.bytes.Finalize()
		outBuf <- readResponse
	}
}

func (r *reader) readChunk() (checked.Bytes, error) {
	// Read size of message
	size, err := binary.ReadUvarint(r.chunkReader)
	if err != nil {
		return nil, err
	}

	// Extend buffer as necessary
	if len(r.dataBuffer) < int(size) {
		diff := int(size) - len(r.dataBuffer)
		for i := 0; i < diff; i++ {
			r.dataBuffer = append(r.dataBuffer, 0)
		}
	}

	// Size the target buffer for reading and unmarshalling
	buffer := r.dataBuffer[:size]

	// Read message
	if _, err := r.chunkReader.Read(buffer); err != nil {
		return nil, err
	}

	// Copy message into pooled byte slice. Reading happens synchronously so its
	// safe to reuse the same buffer repeatedly, but decoding happens in parallel
	// so we need to make sure that the byte slice we return won't be mutated
	// until the caller is done with it.
	pooled := r.bytesPool.Get(int(size))
	pooled.IncRef()
	pooled.AppendAll(buffer)
	pooled.DecRef()
	return pooled, nil
}

func (r *reader) readInfo() (schema.LogInfo, error) {
	data, err := r.readChunk()
	if err != nil {
		return emptyLogInfo, err
	}
	data.IncRef()
	r.logDecoder.Reset(data.Get())
	logInfo, err := r.logDecoder.DecodeLogInfo()
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
	return <-r.shutdownCh
}

func (r *reader) close() error {
	if r.chunkReader.fd == nil {
		return nil
	}
	return r.chunkReader.fd.Close()
}

type chunkReader struct {
	fd        *os.File
	buffer    *bufio.Reader
	remaining int
	charBuff  []byte
}

func newChunkReader(bufferLen int) *chunkReader {
	return &chunkReader{
		buffer:   bufio.NewReaderSize(nil, bufferLen),
		charBuff: make([]byte, 1),
	}
}

func (r *chunkReader) reset(fd *os.File) {
	r.fd = fd
	r.buffer.Reset(fd)
	r.remaining = 0
}

func (r *chunkReader) readHeader() error {
	header, err := r.buffer.Peek(chunkHeaderLen)
	if err != nil {
		return err
	}

	sizeStart, sizeEnd :=
		0, chunkHeaderSizeLen
	checksumSizeStart, checksumSizeEnd :=
		sizeEnd, sizeEnd+chunkHeaderSizeLen
	checksumDataStart, checksumDataEnd :=
		checksumSizeEnd, checksumSizeEnd+chunkHeaderChecksumDataLen

	size := endianness.Uint32(header[sizeStart:sizeEnd])
	checksumSize := digest.
		Buffer(header[checksumSizeStart:checksumSizeEnd]).
		ReadDigest()
	checksumData := digest.
		Buffer(header[checksumDataStart:checksumDataEnd]).
		ReadDigest()

	// Verify size checksum
	if digest.Checksum(header[:4]) != checksumSize {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Discard the peeked header
	if _, err := r.buffer.Discard(chunkHeaderLen); err != nil {
		return err
	}

	// Verify data checksum
	data, err := r.buffer.Peek(int(size))
	if err != nil {
		return err
	}

	if digest.Checksum(data) != checksumData {
		return errCommitLogReaderChunkSizeChecksumMismatch
	}

	// Set remaining data to be consumed
	r.remaining = int(size)

	return nil
}

func (r *chunkReader) Read(p []byte) (int, error) {
	size := len(p)
	read := 0
	// Check if requesting for size larger than this chunk
	if r.remaining < size {
		// Copy any remaining
		if r.remaining > 0 {
			n, err := r.buffer.Read(p[:r.remaining])
			r.remaining -= n
			read += n
			if err != nil {
				return read, err
			}
		}

		// Read next header
		if err := r.readHeader(); err != nil {
			return read, err
		}

		// Reset read target
		p = p[read:]

		// Perform consecutive read(s)
		n, err := r.Read(p)
		read += n
		return read, err
	}

	n, err := r.buffer.Read(p)
	r.remaining -= n
	read += n
	return read, err
}

func (r *chunkReader) ReadByte() (c byte, err error) {
	if _, err := r.Read(r.charBuff); err != nil {
		return byte(0), err
	}
	return r.charBuff[0], nil
}
