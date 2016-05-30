package fs2

import (
	"math"
	"os"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs2/proto"

	"github.com/golang/protobuf/proto"
)

// Writer writes protobuf encoded data entries to a file.
type Writer struct {
	indexFd    *os.File
	dataFd     *os.File
	sizeBuf    *proto.Buffer
	dataBuf    *proto.Buffer
	indexEntry *pb.IndexEntry
	index      map[string]int32
	currIdx    int32
}

// NewWriter creates a writer.
func NewWriter(indexFilePath, dataFilePath string) (*Writer, error) {
	indexFd, err := os.OpenFile(indexFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return nil, err
	}
	dataFd, err := os.OpenFile(dataFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return nil, err
	}
	return &Writer{
		indexFd:    indexFd,
		dataFd:     dataFd,
		sizeBuf:    proto.NewBuffer(nil),
		dataBuf:    proto.NewBuffer(nil),
		indexEntry: &pb.IndexEntry{},
		index:      make(map[string]int32),
	}, nil
}

// Write writes a data entry.
func (w *Writer) Write(id string, timestamp int64, value float64) error {
	idx, ok := w.index[id]
	if !ok {
		idx = w.currIdx
		w.currIdx++
		w.index[id] = idx

		w.indexEntry.Reset()
		w.indexEntry.Id = id
		w.indexEntry.Idx = idx

		w.sizeBuf.Reset()
		w.dataBuf.Reset()

		if err := w.dataBuf.Marshal(w.indexEntry); err != nil {
			return err
		}
		entryBytes := w.dataBuf.Bytes()
		if err := w.sizeBuf.EncodeFixed32(uint64(len(entryBytes))); err != nil {
			return err
		}
		if _, err := w.indexFd.Write(w.sizeBuf.Bytes()); err != nil {
			return err
		}
		if _, err := w.indexFd.Write(entryBytes); err != nil {
			return err
		}
	}

	w.dataBuf.Reset()

	if err := w.dataBuf.EncodeFixed32(uint64(idx)); err != nil {
		return err
	}
	if err := w.dataBuf.EncodeFixed64(uint64(timestamp)); err != nil {
		return err
	}
	if err := w.dataBuf.EncodeFixed64(math.Float64bits(value)); err != nil {
		return err
	}
	if _, err := w.dataFd.Write(w.dataBuf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeEOFMarkers() error {
	fds := []*os.File{w.indexFd, w.dataFd}
	for _, fd := range fds {
		if err := w.writeEOFMarker(fd); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeEOFMarker(fd *os.File) error {
	w.sizeBuf.Reset()
	if err := w.sizeBuf.EncodeFixed32(math.MaxUint32); err != nil {
		return err
	}
	if _, err := fd.Write(w.sizeBuf.Bytes()); err != nil {
		return err
	}
	return nil
}

// Close closes the file with the eof marker appended.
func (w *Writer) Close() error {
	if err := w.writeEOFMarkers(); err != nil {
		return err
	}
	w.indexFd.Close()
	w.dataFd.Close()
	return nil
}
