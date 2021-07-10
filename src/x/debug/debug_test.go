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
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

type fakeSource struct {
	called    bool
	shouldErr bool
	content   string
}

func (f *fakeSource) Write(w io.Writer, _ *http.Request) error {
	f.called = true
	if f.shouldErr {
		return errors.New("bad write")
	}
	w.Write([]byte(f.content))
	return nil
}

func TestWriteZip(t *testing.T) {
	zipWriter := NewZipWriter(instrument.NewOptions())
	fs1 := &fakeSource{
		content: "content1",
	}
	fs2 := &fakeSource{
		content: "content2",
	}
	fs3 := &fakeSource{
		content: "",
	}
	zipWriter.RegisterSource("test1", fs1)
	zipWriter.RegisterSource("test2", fs2)
	zipWriter.RegisterSource("test3", fs3)
	buff := bytes.NewBuffer([]byte{})
	err := zipWriter.WriteZip(buff, &http.Request{})

	bytesReader := bytes.NewReader(buff.Bytes())
	readerCloser, zerr := zip.NewReader(bytesReader, int64(len(buff.Bytes())))

	require.NoError(t, zerr)
	for _, f := range readerCloser.File {
		var expectedContent string
		if f.Name == "test1" {
			expectedContent = "content1"
		} else if f.Name == "test2" {
			expectedContent = "content2"
		} else if f.Name == "test3" {
			expectedContent = ""
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
	zipWriter := NewZipWriter(instrument.NewOptions())
	fs := &fakeSource{
		shouldErr: true,
	}
	zipWriter.RegisterSource("test", fs)
	buff := bytes.NewBuffer([]byte{})
	err := zipWriter.WriteZip(buff, &http.Request{})
	require.Error(t, err)
	require.True(t, fs.called)
}

func TestRegisterSourceSameName(t *testing.T) {
	zipWriter := NewZipWriter(instrument.NewOptions())
	fs := &fakeSource{}
	err := zipWriter.RegisterSource("test", fs)
	require.NoError(t, err)
	err = zipWriter.RegisterSource("test", fs)
	require.Error(t, err)
}

func TestHTTPEndpoint(t *testing.T) {
	mux := http.NewServeMux()

	// Randomizing the path here so we avoid multiple tests
	// registering the same endpoint.
	path := fmt.Sprintf("/debug/%s", randStringBytes(10))

	zw := NewZipWriter(instrument.NewOptions())
	fs1 := &fakeSource{
		content: "test",
	}
	fs2 := &fakeSource{
		content: "bar",
	}
	err := zw.RegisterSource("test", fs1)
	require.NoError(t, err)
	err = zw.RegisterSource("foo", fs2)
	require.NoError(t, err)

	err = zw.RegisterHandler(path, mux)
	require.NoError(t, err)

	buf := bytes.NewBuffer([]byte{})
	req, err := http.NewRequest("GET", path, buf)
	require.NoError(t, err)

	t.Run("TestDownloadZip", func(t *testing.T) {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		require.NotZero(t, rr.Body.Len())
		rawResponse := make([]byte, rr.Body.Len())
		n, err := rr.Body.Read(rawResponse)
		require.NoError(t, err)
		require.NotZero(t, n)
		require.Equal(t, rr.Code, http.StatusOK)

		bytesReader := bytes.NewReader(rawResponse)
		zipReader, err := zip.NewReader(bytesReader, int64(bytesReader.Len()))
		require.NoError(t, err)
		require.NotNil(t, zipReader)
		for _, f := range zipReader.File {
			f := f
			t.Run(f.Name, func(t *testing.T) {
				var expectedContent string
				switch {
				case f.Name == "test":
					expectedContent = "test"
				case f.Name == "foo":
					expectedContent = "bar"
				default:
					t.Errorf("bad filename from archive %s", f.Name)
				}

				rc, ferr := f.Open()
				require.NoError(t, ferr)
				defer func() {
					require.NoError(t, rc.Close())
				}()

				content := make([]byte, len(expectedContent))
				_, err = rc.Read(content)
				if assert.Error(t, err) {
					require.Equal(t, err, io.EOF)
				}
				require.Equal(t, expectedContent, string(content))
			})
		}
	})

	t.Run("TestDownloadZipFail", func(t *testing.T) {
		fs3 := &fakeSource{
			content:   "oh snap",
			shouldErr: true,
		}
		zw.RegisterSource("test2", fs3)

		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		require.Equal(t, rr.Code, http.StatusInternalServerError)
	})
}
