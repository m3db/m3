// Copyright (c) 2018 Uber Technologies, Inc.
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

package clone

import (
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/ident/testutil"
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
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(src.Namespace),
			Shard:      src.Shard,
			BlockStart: src.Blockstart,
		},
		FileSetType: persist.FileSetFlushType,
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
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(dest.Namespace),
			Shard:      dest.Shard,
			BlockStart: dest.Blockstart,
		},
	}
	if err := writer.Open(writerOpts); err != nil {
		return fmt.Errorf("unable to open fileset writer: %v", err)
	}

	for {
		id, tagsIter, data, checksum, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unexpected error while reading data: %v", err)
		}

		tags, err := testutil.NewTagsFromTagIterator(tagsIter)
		if err != nil {
			return err
		}

		data.IncRef()
		if err := writer.Write(id, tags, data, checksum); err != nil {
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
