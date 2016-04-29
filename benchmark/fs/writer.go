package fs

import (
	"os"

	pb "code.uber.internal/infra/memtsdb/benchmark/fs/proto"
	"github.com/golang/protobuf/proto"
)

// Writer writes protobuf encoded data entries to a file.
type Writer struct {
	fd      *os.File
	sizeBuf *proto.Buffer
	dataBuf *proto.Buffer
}

// NewWriter creates a writer.
func NewWriter(filePath string) (*Writer, error) {
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return nil, err
	}
	return &Writer{fd: fd, sizeBuf: proto.NewBuffer(nil), dataBuf: proto.NewBuffer(nil)}, nil
}

// Write writes a data entry.
func (w *Writer) Write(entry *pb.DataEntry) error {
	w.sizeBuf.Reset()
	w.dataBuf.Reset()
	if err := w.dataBuf.Marshal(entry); err != nil {
		return err
	}
	entryBytes := w.dataBuf.Bytes()
	if err := w.sizeBuf.EncodeVarint(uint64(len(entryBytes))); err != nil {
		return err
	}
	if _, err := w.fd.Write(w.sizeBuf.Bytes()); err != nil {
		return err
	}
	if _, err := w.fd.Write(entryBytes); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeEOFMarker() error {
	if _, err := w.fd.Write(proto.EncodeVarint(eofMarker)); err != nil {
		return err
	}
	return nil
}

// Close closes the file with the eof marker appended.
func (w *Writer) Close() error {
	if err := w.writeEOFMarker(); err != nil {
		return err
	}
	return w.fd.Close()
}
