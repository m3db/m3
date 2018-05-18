// Copyright (c) 2017 Uber Technologies, Inc.
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

package commitlog

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist/fs/msgpack"
)

// ReadLogInfo reads the commit log info out of a commitlog file
func ReadLogInfo(filePath string, opts Options) (time.Time, time.Duration, int64, error) {
	var fd *os.File
	var err error
	defer func() {
		if fd == nil {
			fd.Close()
		}
	}()

	fd, err = os.Open(filePath)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	chunkReader := newChunkReader(opts.FlushSize())
	chunkReader.reset(fd)
	size, err := binary.ReadUvarint(chunkReader)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	bytes := make([]byte, size)
	_, err = chunkReader.Read(bytes)
	if err != nil {
		return time.Time{}, 0, 0, err
	}
	logDecoder := msgpack.NewDecoder(nil)
	logDecoder.Reset(msgpack.NewDecoderStream(bytes))
	logInfo, decoderErr := logDecoder.DecodeLogInfo()

	err = fd.Close()
	fd = nil
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	return time.Unix(0, logInfo.Start), time.Duration(logInfo.Duration), logInfo.Index, decoderErr
}
