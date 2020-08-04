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

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
)

func TestPlacementSource(t *testing.T) {
	handlerOpts, _ := newHandlerOptsAndClient(t)
	iOpts := instrument.NewOptions()
	svcDefaults := handleroptions.ServiceNameAndDefaults{
		ServiceName: "m3db",
	}
	p, err := NewPlacementInfoSource(svcDefaults, handlerOpts, iOpts)
	require.NoError(t, err)

	buff := bytes.NewBuffer([]byte{})
	p.Write(buff, &http.Request{})
	require.NotZero(t, buff.Len())
}
