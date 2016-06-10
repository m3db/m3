package fs2

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs2/proto"
	"github.com/golang/protobuf/proto"
)

// Reader reads protobuf encoded data entries from a file.
type Reader struct {
	indexFd   *os.File
	dataFd    *os.File
	indexData []byte
	dataData  []byte
}

// NewReader creates a reader.
func NewReader(indexFile, dataFile string) (*Reader, error) {
	indexFd, err := os.Open(indexFile)
	if err != nil {
		return nil, fmt.Errorf("could not open index file %s: %v", indexFile, err)
	}
	dataFd, err := os.Open(dataFile)
	if err != nil {
		return nil, fmt.Errorf("could not open data file %s: %v", dataFile, err)
	}
	return &Reader{indexFd: indexFd, dataFd: dataFd}, nil
}

// Close closes the files.
func (r *Reader) Close() error {
	var resultErr error
	fds := []*os.File{r.indexFd, r.dataFd}
	for _, fd := range fds {
		if err := fd.Close(); err != nil && resultErr == nil {
			resultErr = err
		}
	}
	return resultErr
}

// Iter creates an iterator.
func (r *Reader) Iter() (*Iterator, error) {
	if err := r.ensureReadIndex(); err != nil {
		return nil, err
	}
	if err := r.ensureReadData(); err != nil {
		return nil, err
	}
	return newIterator(bytes.NewReader(r.indexData), bytes.NewReader(r.dataData)), nil
}

// IterIDs creates an ID iterator.
func (r *Reader) IterIDs() (*IDIterator, error) {
	if err := r.ensureReadIndex(); err != nil {
		return nil, err
	}
	return newIDIterator(bytes.NewReader(r.indexData)), nil
}

func (r *Reader) ensureReadIndex() error {
	var err error
	if r.indexData == nil {
		r.indexData, err = ioutil.ReadAll(r.indexFd)
	}
	return err
}

func (r *Reader) ensureReadData() error {
	var err error
	if r.dataData == nil {
		r.dataData, err = ioutil.ReadAll(r.dataFd)
	}
	return err
}

// Iterator iterates over protobuf encoded data entries.
type Iterator struct {
	indexFd       io.Reader
	dataFd        io.Reader
	index         map[int32]string
	indexEntry    *pb.IndexEntry
	dataBuf       *proto.Buffer
	bd            *binaryDecoder
	indexEntryBuf []byte
	id            string
	timestamp     int64
	value         float64
	done          bool
	err           error
}

func newIterator(indexFd, dataFd io.Reader) *Iterator {
	return &Iterator{
		indexFd:    indexFd,
		dataFd:     dataFd,
		index:      make(map[int32]string),
		indexEntry: &pb.IndexEntry{},
		dataBuf:    proto.NewBuffer(nil),
		bd:         newBinaryDecoder(),
	}
}

// Next moves to the next data entry.
func (it *Iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	unsignedIdx, err := it.bd.readUInt32(it.dataFd)
	if err != nil {
		it.err = err
		return false
	}
	if unsignedIdx == math.MaxUint32 {
		it.done = true
		return false
	}

	idx := int32(unsignedIdx)

	id, ok := it.index[idx]
	if !ok {
		// Will be the very next one in the index file
		indexEntryLen, err := it.bd.readUInt32(it.indexFd)
		if err != nil {
			it.err = err
			return false
		}
		if indexEntryLen == math.MaxUint32 {
			it.err = fmt.Errorf("encountered end of index when expected next index entry")
			return false
		}

		if len(it.indexEntryBuf) < int(indexEntryLen) {
			it.indexEntryBuf = make([]byte, indexEntryLen)
		}
		readBuf := it.indexEntryBuf[:indexEntryLen]
		n, err := it.indexFd.Read(readBuf)
		if err != nil {
			it.err = err
			return false
		}
		if n != int(indexEntryLen) {
			it.err = fmt.Errorf("could not read index entry")
			return false
		}

		it.dataBuf.SetBuf(readBuf)
		if err := it.dataBuf.Unmarshal(it.indexEntry); err != nil {
			it.err = err
			return false
		}
		if it.indexEntry.Idx != idx {
			it.err = fmt.Errorf(
				"unexpected index entry, idx %d does not match expected idx %d",
				it.indexEntry.Idx, idx)
			return false
		}
		id = it.indexEntry.Id
		it.index[idx] = id
	}

	timestamp, err := it.bd.readUInt64(it.dataFd)
	if err != nil {
		it.err = err
		return false
	}

	value, err := it.bd.readUInt64(it.dataFd)
	if err != nil {
		it.err = err
		return false
	}

	it.id = id
	it.timestamp = int64(timestamp)
	it.value = math.Float64frombits(value)

	return true
}

// Value returns the current entry.
func (it *Iterator) Value() (string, int64, float64) {
	return it.id, it.timestamp, it.value
}

// Err returns the current error.
func (it *Iterator) Err() error {
	return it.err
}

// IDIterator iterates over protobuf encoded data entries.
type IDIterator struct {
	indexFd       io.Reader
	indexEntry    *pb.IndexEntry
	dataBuf       *proto.Buffer
	bd            *binaryDecoder
	indexEntryBuf []byte
	id            string
	idx           int32
	done          bool
	err           error
}

func newIDIterator(indexFd io.Reader) *IDIterator {
	return &IDIterator{
		indexFd:    indexFd,
		indexEntry: &pb.IndexEntry{},
		dataBuf:    proto.NewBuffer(nil),
		bd:         newBinaryDecoder(),
	}
}

// Next moves to the next data entry.
func (it *IDIterator) Next() bool {
	indexEntryLen, err := it.bd.readUInt32(it.indexFd)
	if err != nil {
		it.err = err
		return false
	}
	if indexEntryLen == math.MaxUint32 {
		it.done = true
		return false
	}

	if len(it.indexEntryBuf) < int(indexEntryLen) {
		it.indexEntryBuf = make([]byte, indexEntryLen)
	}
	readBuf := it.indexEntryBuf[:indexEntryLen]
	n, err := it.indexFd.Read(readBuf)
	if err != nil {
		it.err = err
		return false
	}
	if n != int(indexEntryLen) {
		it.err = fmt.Errorf("could not read index entry")
		return false
	}

	it.dataBuf.SetBuf(readBuf)
	if err := it.dataBuf.Unmarshal(it.indexEntry); err != nil {
		it.err = err
		return false
	}

	it.id = it.indexEntry.Id
	it.idx = it.indexEntry.Idx
	return true
}

// Value returns the current entry.
func (it *IDIterator) Value() (string, int32) {
	return it.id, it.idx
}

// Err returns the current error.
func (it *IDIterator) Err() error {
	return it.err
}

type binaryDecoder struct {
	dataBuf   *proto.Buffer
	uint32Buf [4]byte
	uint64Buf [8]byte
}

func newBinaryDecoder() *binaryDecoder {
	return &binaryDecoder{
		dataBuf: proto.NewBuffer(nil),
	}
}

func (d *binaryDecoder) readUInt32(fd io.Reader) (uint32, error) {
	n, err := fd.Read(d.uint32Buf[:])
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("unable to read int32")
	}

	d.dataBuf.SetBuf(d.uint32Buf[:])
	value, err := d.dataBuf.DecodeFixed32()
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}

func (d *binaryDecoder) readUInt64(fd io.Reader) (uint64, error) {
	n, err := fd.Read(d.uint64Buf[:])
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("unable to read int64")
	}

	d.dataBuf.SetBuf(d.uint64Buf[:])
	return d.dataBuf.DecodeFixed64()
}
