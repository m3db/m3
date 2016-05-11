package fs

import (
	"os"

	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"

	"github.com/golang/protobuf/proto"
)

var (
	defaultNewFileMode = os.FileMode(0666)
)

type writer struct {
	infoFd  *os.File
	indexFd *os.File
	dataFd  *os.File

	currEntry    schema.IndexEntry
	currIdx      int64
	currOffset   int64
	indexBuffer  *proto.Buffer
	varintBuffer *proto.Buffer
	idxData      []byte
}

// WriterOptions provides options for a Writer
type WriterOptions struct {
	NewFileMode os.FileMode
}

// DefaultWriterOptions describes options for a Writer
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{NewFileMode: defaultNewFileMode}
}

// NewWriter returns a new writer for a filePathPrefix, will truncate files
// if they exist.
func NewWriter(filePathPrefix string, options WriterOptions) (Writer, error) {
	newFileMode := options.NewFileMode
	if uint32(newFileMode) == 0 {
		newFileMode = defaultNewFileMode
	}
	w := &writer{
		indexBuffer:  proto.NewBuffer(nil),
		varintBuffer: proto.NewBuffer(nil),
		idxData:      make([]byte, idxLen),
	}
	err := openFilesWithFilePathPrefix(writeableFileOpener(newFileMode), map[string]**os.File{
		filenameFromPrefix(filePathPrefix, infoFileSuffix):  &w.infoFd,
		filenameFromPrefix(filePathPrefix, indexFileSuffix): &w.indexFd,
		filenameFromPrefix(filePathPrefix, dataFileSuffix):  &w.dataFd,
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *writer) writeData(data []byte) error {
	written, err := w.dataFd.Write(data)
	if err != nil {
		return err
	}
	w.currOffset += int64(written)
	return nil
}

func (w *writer) Write(key string, data []byte) error {
	idx := w.currIdx

	entry := &w.currEntry
	entry.Reset()
	entry.Idx = idx
	entry.Key = key
	entry.Size = int64(len(data))
	entry.Offset = w.currOffset

	w.indexBuffer.Reset()
	if err := w.indexBuffer.Marshal(entry); err != nil {
		return err
	}

	w.varintBuffer.Reset()
	entryBytes := w.indexBuffer.Bytes()
	if err := w.varintBuffer.EncodeVarint(uint64(len(entryBytes))); err != nil {
		return err
	}

	if err := w.writeData(marker); err != nil {
		return err
	}
	endianness.PutUint64(w.idxData, uint64(idx))
	if err := w.writeData(w.idxData); err != nil {
		return err
	}
	if err := w.writeData(data); err != nil {
		return err
	}

	if _, err := w.indexFd.Write(w.varintBuffer.Bytes()); err != nil {
		return err
	}
	if _, err := w.indexFd.Write(entryBytes); err != nil {
		return err
	}

	w.currIdx++

	return nil
}

func (w *writer) Close() error {
	if err := w.infoFd.Truncate(0); err != nil {
		return err
	}

	info := &schema.IndexInfo{Entries: w.currIdx}
	data, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	if _, err := w.infoFd.Write(data); err != nil {
		return err
	}

	return closeFiles(w.infoFd, w.indexFd, w.dataFd)
}

func writeableFileOpener(fileMode os.FileMode) fileOpener {
	return func(filePath string) (*os.File, error) {
		fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
		if err != nil {
			return nil, err
		}
		return fd, nil
	}
}
