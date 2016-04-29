package fs

import (
	"fmt"
	"io/ioutil"
	"os"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs/proto"
	"github.com/golang/protobuf/proto"
)

// Reader reads protobuf encoded data entries from a file.
type Reader struct {
	buf []byte // raw bytes stored in the file
}

// NewReader creates a reader.
func NewReader(filePath string) (*Reader, error) {
	input, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %v", filePath, err)
	}

	defer input.Close()

	b, err := ioutil.ReadAll(input)
	if err != nil {
		return nil, fmt.Errorf("unable to read all bytes from %s: %v", filePath, err)
	}

	return &Reader{buf: b}, nil
}

// Iter creates an iterator.
func (r *Reader) Iter() *Iterator {
	return newIterator(r.buf)
}

// Iterator iterates over protobuf encoded data entries.
type Iterator struct {
	buf   []byte
	entry *pb.DataEntry
	done  bool
	err   error
}

func newIterator(buf []byte) *Iterator {
	return &Iterator{buf: buf, entry: &pb.DataEntry{}}
}

// Next moves to the next data entry.
func (it *Iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	it.entry.Reset()
	entrySize, consumed := proto.DecodeVarint(it.buf)
	if consumed == 0 {
		it.err = errReadDataEntryNoSize
		return false
	}
	it.buf = it.buf[consumed:]
	if entrySize == eofMarker {
		it.done = true
		return false
	}
	data := it.buf[:entrySize]
	if it.err = proto.Unmarshal(data, it.entry); it.err != nil {
		return false
	}
	it.buf = it.buf[entrySize:]
	return true
}

// Value returns the current entry.
func (it *Iterator) Value() *pb.DataEntry {
	return it.entry
}

// Err returns the current error.
func (it *Iterator) Err() error {
	return it.err
}
