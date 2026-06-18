// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bufio"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
)

func TestChunkWriterWrite(t *testing.T) {
	// This test panics and shows issue #2139:
	// https://github.com/m3db/m3/issues/2139
	// The fundamental cause is that the writer used for the buffered writer
	// (chunkWriter) writes more bytes than the bytes it is given. This panic
	// (but not the root cause) is mitigated by pull #2148:
	// https://github.com/m3db/m3/pull/2148
	t.SkipNow()

	noopFlushFn := func(err error) {}
	writer := newChunkWriter(noopFlushFn, false)
	fd := &mockFile{}
	writer.reset(fd)
	require.True(t, writer.isOpen())

	buffer := bufio.NewWriterSize(nil, 4)
	buffer.Reset(writer)

	payload := []byte("12345")
	n, err := buffer.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
}

// mockFile is a noop mock xos.File that pretends that all operations are always
// successful.
type mockFile struct {
}

func (m *mockFile) Write(p []byte) (int, error) {
	return len(p), nil
}

func (m *mockFile) Sync() error {
	return nil
}

func (m *mockFile) Close() error {
	return nil
}

func TestCommitLogWriterLargeWriteNoPanic(t *testing.T) {
	noopFlushFn := func(err error) {}
	writer := newCommitLogWriter(noopFlushFn, testOpts.SetFlushSize(50))
	series := testSeries(0, "id0", testTags1, 0)
	datapoint := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}
	// This write size is ~80 bytes (flush size set to 50). This should return
	// an error and not panic.
	require.Error(t, writer.Write(series, datapoint, xtime.Millisecond, []byte("1234567890")))
}
