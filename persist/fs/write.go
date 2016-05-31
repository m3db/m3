package fs

import (
	"os"
	"time"

	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/golang/protobuf/proto"
)

var (
	defaultNewFileMode      = os.FileMode(0666)
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)
)

type writer struct {
	start            time.Time
	window           time.Duration
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode

	infoFd  *os.File
	indexFd *os.File
	dataFd  *os.File

	currEntry    schema.IndexEntry
	currIdx      int64
	currOffset   int64
	infoBuffer   *proto.Buffer
	indexBuffer  *proto.Buffer
	varintBuffer *proto.Buffer
	idxData      []byte
}

// WriterOptions provides options for a Writer
type WriterOptions interface {
	// NewFileMode sets the new file mode.
	NewFileMode(value os.FileMode) WriterOptions

	// GetNewFileMode returns the new file mode.
	GetNewFileMode() os.FileMode

	// NewDirectoryMode sets the new directory mode.
	NewDirectoryMode(value os.FileMode) WriterOptions

	// GetNewDirectoryMode returns the new directory mode.
	GetNewDirectoryMode() os.FileMode
}

type writerOptions struct {
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
}

// NewWriterOptions creates a writer options.
func NewWriterOptions() WriterOptions {
	return &writerOptions{
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
	}
}

func (o *writerOptions) NewFileMode(value os.FileMode) WriterOptions {
	opts := *o
	opts.newFileMode = value
	return &opts
}

func (o *writerOptions) GetNewFileMode() os.FileMode {
	return o.newFileMode
}

func (o *writerOptions) NewDirectoryMode(value os.FileMode) WriterOptions {
	opts := *o
	opts.newDirectoryMode = value
	return &opts
}

func (o *writerOptions) GetNewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}

// NewWriter returns a new writer for a filePathPrefix
func NewWriter(
	start time.Time,
	window time.Duration,
	filePathPrefix string,
	options WriterOptions,
) Writer {
	if options == nil {
		options = NewWriterOptions()
	}
	return &writer{
		start:            start,
		window:           window,
		filePathPrefix:   filePathPrefix,
		newFileMode:      options.GetNewFileMode(),
		newDirectoryMode: options.GetNewDirectoryMode(),
		infoBuffer:       proto.NewBuffer(nil),
		indexBuffer:      proto.NewBuffer(nil),
		varintBuffer:     proto.NewBuffer(nil),
		idxData:          make([]byte, idxLen),
	}
}

// Open initializes the internal state for writing to the given shard,
// specifically creating the shard directory if it doesn't exist, and
// opening / truncating files associated with that shard for writing.
func (w *writer) Open(shard uint32) error {
	shardDir := ShardDirPath(w.filePathPrefix, shard)
	if err := os.MkdirAll(shardDir, w.newDirectoryMode); err != nil {
		return err
	}
	nextVersion, err := nextVersion(shardDir)
	if err != nil {
		return err
	}
	return openFiles(
		writeableFileOpener(w.newFileMode),
		map[string]**os.File{
			filepathFromVersion(shardDir, nextVersion, infoFileSuffix):  &w.infoFd,
			filepathFromVersion(shardDir, nextVersion, indexFileSuffix): &w.indexFd,
			filepathFromVersion(shardDir, nextVersion, dataFileSuffix):  &w.dataFd,
		},
	)
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

	info := &schema.IndexInfo{
		Start:   xtime.ToNanoseconds(w.start),
		Window:  int64(w.window),
		Entries: w.currIdx,
	}
	if err := w.infoBuffer.Marshal(info); err != nil {
		return err
	}

	if _, err := w.infoFd.Write(w.infoBuffer.Bytes()); err != nil {
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
