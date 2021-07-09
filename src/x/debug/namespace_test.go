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
	"bytes"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
)

func TestNamespaceSource(t *testing.T) {
	_, mockKV, mockClient := newHandlerOptsAndClient(t)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil)
	iOpts := instrument.NewOptions()
	n, err := NewNamespaceInfoSource(mockClient, []handleroptions.ServiceNameAndDefaults{
		{
			ServiceName: handleroptions.M3DBServiceName,
		},
	}, iOpts)
	require.NoError(t, err)

	buff := bytes.NewBuffer([]byte{})
	n.Write(buff, &http.Request{})
	require.NotZero(t, buff.Len())
}
