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

	"github.com/m3db/m3db/persist/encoding/msgpack"
)

// ReadLogInfo reads the commit log info out of a commitlog file
func ReadLogInfo(filePath string) (start time.Time, duration time.Duration, index int64, err error) {
	fd, err := os.Open(filePath)
	defer fd.Close()
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	chunkReader := newChunkReader(defaultFlushSize)
	chunkReader.reset(fd)
	size, err := binary.ReadUvarint(chunkReader)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	bytes := make([]byte, size, size)
	_, err = chunkReader.Read(bytes)
	if err != nil {
		return time.Time{}, 0, 0, err
	}
	logDecoder := msgpack.NewDecoder(nil)
	logDecoder.Reset(bytes)
	logInfo, err := logDecoder.DecodeLogInfo()
	return time.Unix(0, logInfo.Start), time.Duration(logInfo.Duration), logInfo.Index, err
}
