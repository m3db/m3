// Copyright (c) 2021 Uber Technologies, Inc.
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

package extdebug

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	debugtest "github.com/m3db/m3/src/x/debug/test"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDefaultSources(t *testing.T) {
	defaultSources := []string{
		"cpu.prof",
		"heap.prof",
		"host.json",
		"goroutine.prof",
		"namespace.json",
		"placement-m3db.json",
	}

	handlerOpts, mockKV, mockClient := debugtest.NewTestHandlerOptsAndClient(t)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)
	svcDefaults := []handleroptions.ServiceNameAndDefaults{{
		ServiceName: handleroptions.M3DBServiceName,
	}}
	zw, err := NewPlacementAndNamespaceZipWriterWithDefaultSources(
		1*time.Second, mockClient, handlerOpts, svcDefaults, instrument.NewOptions())
	require.NoError(t, err)
	require.NotNil(t, zw)

	// Check writing ZIP is ok
	buff := bytes.NewBuffer([]byte{})
	err = zw.WriteZip(buff, &http.Request{})
	require.NoError(t, err)
	require.NotZero(t, buff.Len())

	// Check written ZIP is not empty
	bytesReader := bytes.NewReader(buff.Bytes())
	zipReader, err := zip.NewReader(bytesReader, int64(bytesReader.Len()))
	require.NoError(t, err)
	require.NotNil(t, zipReader)

	actualFnames := make(map[string]bool)
	for _, f := range zipReader.File {
		actualFnames[f.Name] = true

		rc, ferr := f.Open()
		require.NoError(t, ferr)
		defer func() {
			require.NoError(t, rc.Close())
		}()

		content := []byte{}
		if _, err := rc.Read(content); err != nil {
			require.Equal(t, io.EOF, err)
		}
		require.NotZero(t, content)
	}

	for _, source := range defaultSources {
		_, ok := actualFnames[source]
		require.True(t, ok)
	}
}
