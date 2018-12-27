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

package debugdump

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeProvider struct {
	called    bool
	shouldErr bool
}

func (f *fakeProvider) ProvideData(w io.Writer) error {
	f.called = true
	if f.shouldErr {
		return errors.New("bad provide")
	}
	w.Write([]byte("test"))
	return nil
}

func TestDumpData(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{}
	dataDumper.RegisterProvider("test", fp)
	buff := bytes.NewBuffer([]byte{})
	err := dataDumper.DumpData(buff)
	require.NotZero(t, buff.Len())
	require.True(t, fp.called)
	require.NoError(t, err)
}

func TestDumpDataErr(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{
		shouldErr: true,
	}
	dataDumper.RegisterProvider("test", fp)
	buff := bytes.NewBuffer([]byte{})
	err := dataDumper.DumpData(buff)
	require.Error(t, err)
	require.True(t, fp.called)
}

func TestRegisterProviderSameName(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{}
	err := dataDumper.RegisterProvider("test", fp)
	require.NoError(t, err)
	err = dataDumper.RegisterProvider("test", fp)
	require.Error(t, err)
}
