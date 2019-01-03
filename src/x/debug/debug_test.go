// Copyright (c) 2019 Uber Technologies, Inc.
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

package debug

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeSource struct {
	called    bool
	shouldErr bool
	content   string
}

func (f *fakeSource) Write(w io.Writer) error {
	f.called = true
	if f.shouldErr {
		return errors.New("bad write")
	}
	w.Write([]byte(f.content))
	return nil
}

func TestWriteZip(t *testing.T) {
	zipWriter := NewZipWriter()
	fs1 := &fakeSource{
		content: "content1",
	}
	fs2 := &fakeSource{
		content: "content2",
	}
	zipWriter.RegisterSource("test1", fs1)
	zipWriter.RegisterSource("test2", fs2)
	buff := bytes.NewBuffer([]byte{})
	err := zipWriter.WriteZip(buff)

	bytesReader := bytes.NewReader(buff.Bytes())
	readerCloser, zerr := zip.NewReader(bytesReader, int64(len(buff.Bytes())))

	require.NoError(t, zerr)
	for _, f := range readerCloser.File {
		var expectedContent string
		if f.Name == "test1" {
			expectedContent = "content1"
		} else if f.Name == "test2" {
			expectedContent = "content2"
		} else {
			t.Errorf("bad filename from archive %s", f.Name)
		}

		rc, ferr := f.Open()
		require.NoError(t, ferr)
		content := make([]byte, len(expectedContent))
		rc.Read(content)
		require.Equal(t, expectedContent, string(content))
	}

	require.True(t, fs1.called)
	require.True(t, fs2.called)
	require.NoError(t, err)
	require.NotZero(t, buff.Len())
}

func TestWriteZipErr(t *testing.T) {
	zipWriter := NewZipWriter()
	fs := &fakeSource{
		shouldErr: true,
	}
	zipWriter.RegisterSource("test", fs)
	buff := bytes.NewBuffer([]byte{})
	err := zipWriter.WriteZip(buff)
	require.Error(t, err)
	require.True(t, fs.called)
}

func TestRegisterSourceSameName(t *testing.T) {
	zipWriter := NewZipWriter()
	fs := &fakeSource{}
	err := zipWriter.RegisterSource("test", fs)
	require.NoError(t, err)
	err = zipWriter.RegisterSource("test", fs)
	require.Error(t, err)
}
