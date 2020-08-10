package fs

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
)

type LargeTilesWriter interface {
	Open(encoder encoding.Encoder) error
	Write(ctx context.Context, encoder encoding.Encoder, id ident.ID, tags ident.TagIterator) error
	Close() error
}

type LargeTilesWriterOptions struct {
	NamespaceID      ident.ID
	ShardID          uint32
	FilePathPrefix   string
	WriterBufferSize int
	BlockStart       time.Time
	BlockSize        time.Duration
}

type largeTilesWriter struct {
	opts       LargeTilesWriterOptions
	writer     DataFileSetWriter
	writerOpts DataWriterOpenOptions
	data       []checked.Bytes
}

func NewLargeTilesWriter(opts LargeTilesWriterOptions) (LargeTilesWriter, error) {
	writer, err := NewWriter(NewOptions().
		SetFilePathPrefix(opts.FilePathPrefix).
		SetWriterBufferSize(opts.WriterBufferSize),
	)
	if err != nil {
		return nil, err
	}

	writerOpts := DataWriterOpenOptions{
		BlockSize: opts.BlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  opts.NamespaceID,
			Shard:      opts.ShardID,
			BlockStart: opts.BlockStart,
		},
		FileSetType: persist.FileSetFlushType,
	}

	return &largeTilesWriter{
		opts:       opts,
		writer:     writer,
		writerOpts: writerOpts,
		data:       make([]checked.Bytes, 2),
	}, nil
}

func (w *largeTilesWriter) Open(encoder encoding.Encoder) error {
	return w.writer.Open(w.writerOpts)
}

func (w *largeTilesWriter) Write(
	ctx context.Context,
	encoder encoding.Encoder,
	id ident.ID,
	tags ident.TagIterator,
) error {
	stream, ok := encoder.Stream(ctx)
	if !ok {
		// None of the datapoints passed the predicate.
		return nil
	}
	segment, err := stream.Segment()
	if err != nil {
		return err
	}
	w.data[0] = segment.Head
	w.data[1] = segment.Tail
	checksum := segment.CalculateChecksum()

	// FIXME: remove this conversion
	tgs := make([]ident.Tag, 0, tags.Len())
	for tags.Next() {
		t := tags.Current()
		tgs = append(tgs, ident.StringTag(t.Name.String(), t.Value.String()))
	}

	metadata := persist.NewMetadataFromIDAndTags(id, ident.NewTags(tgs...),
		persist.MetadataOptions{})
	return w.writer.WriteAll(metadata, w.data, checksum)
}

func (w *largeTilesWriter) Close() error {
	return w.writer.Close()
}
