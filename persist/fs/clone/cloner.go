package clone

import (
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/ident"
)

type cloner struct {
	opts Options
}

// New creates a new fileset cloner
func New(opts Options) FileSetCloner {
	return &cloner{
		opts: opts,
	}
}

func (c *cloner) Clone(src FileSetID, dest FileSetID, destBlocksize time.Duration) error {
	fsopts := fs.NewOptions().
		SetDataReaderBufferSize(c.opts.BufferSize()).
		SetInfoReaderBufferSize(c.opts.BufferSize()).
		SetWriterBufferSize(c.opts.BufferSize()).
		SetNewFileMode(c.opts.FileMode()).
		SetNewDirectoryMode(c.opts.DirMode())
	reader, err := fs.NewReader(nil, fsopts.SetFilePathPrefix(src.PathPrefix))
	if err != nil {
		return fmt.Errorf("unable to create fileset reader: %v", err)
	}
	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FilesetFileIdentifier{
			Namespace:  ident.StringID(src.Namespace),
			Shard:      src.Shard,
			BlockStart: src.Blockstart,
		},
		FilesetType: persist.FilesetFlushType,
	}

	if err := reader.Open(openOpts); err != nil {
		return fmt.Errorf("unable to read source fileset: %v", err)
	}

	writer, err := fs.NewWriter(fsopts.SetFilePathPrefix(dest.PathPrefix))
	if err != nil {
		return fmt.Errorf("unable to create fileset writer: %v", err)
	}
	writerOpts := fs.DataWriterOpenOptions{
		BlockSize: destBlocksize,
		Identifier: fs.FilesetFileIdentifier{
			Namespace:  ident.StringID(dest.Namespace),
			Shard:      dest.Shard,
			BlockStart: dest.Blockstart,
		},
	}
	if err := writer.Open(writerOpts); err != nil {
		return fmt.Errorf("unable to open fileset writer: %v", err)
	}

	for {
		id, data, checksum, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unexpected error while reading data: %v", err)
		}

		data.IncRef()
		if err := writer.Write(id, data, checksum); err != nil {
			return fmt.Errorf("unexpected error while writing data: %v", err)
		}
		data.DecRef()
		data.Finalize()
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("unable to finalize writer: %v", err)
	}

	if err := reader.Close(); err != nil {
		return fmt.Errorf("unable to finalize reader: %v", err)
	}

	return nil
}
