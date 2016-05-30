package fs2

import (
	"fmt"
	"math"
	"os"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs2/proto"
	"github.com/golang/protobuf/proto"
)

// Reader reads protobuf encoded data entries from a file.
type Reader struct {
	indexFd *os.File
	dataFd  *os.File
}

// NewReader creates a reader.
func NewReader(indexFd, dataFd *os.File) (*Reader, error) {
	return &Reader{indexFd, dataFd}, nil
}

// Iter creates an iterator.
func (r *Reader) Iter() *Iterator {
	return newIterator(r.indexFd, r.dataFd)
}

// Iterator iterates over protobuf encoded data entries.
type Iterator struct {
	indexFd       *os.File
	dataFd        *os.File
	index         map[int32]string
	indexEntry    *pb.IndexEntry
	dataBuf       *proto.Buffer
	uint32Buf     [4]byte
	uint64Buf     [8]byte
	indexEntryBuf []byte
	id            string
	timestamp     int64
	value         float64
	done          bool
	err           error
}

func newIterator(indexFd, dataFd *os.File) *Iterator {
	return &Iterator{
		indexFd:    indexFd,
		dataFd:     dataFd,
		index:      make(map[int32]string),
		indexEntry: &pb.IndexEntry{},
		dataBuf:    proto.NewBuffer(nil),
	}
}

// Next moves to the next data entry.
func (it *Iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	unsignedIdx, err := it.readUInt32(it.dataFd)
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
		indexEntryLen, err := it.readUInt32(it.indexFd)
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

	timestamp, err := it.readUInt64(it.dataFd)
	if err != nil {
		it.err = err
		return false
	}

	value, err := it.readUInt64(it.dataFd)
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

func (it *Iterator) readUInt32(fd *os.File) (uint32, error) {
	n, err := fd.Read(it.uint32Buf[:])
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("unable to read int32")
	}

	it.dataBuf.SetBuf(it.uint32Buf[:])
	value, err := it.dataBuf.DecodeFixed32()
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}

func (it *Iterator) readUInt64(fd *os.File) (uint64, error) {
	n, err := fd.Read(it.uint64Buf[:])
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("unable to read int64")
	}

	it.dataBuf.SetBuf(it.uint64Buf[:])
	return it.dataBuf.DecodeFixed64()
}
