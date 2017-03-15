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

package thriftudp

import (
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/stretchr/testify/assert"
)

func TestNewTMultiUDPClientTransport(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{"127.0.0.1:9090", "127.0.0.1:9090"}, "")
	assert.NotNil(t, trans)
	assert.Nil(t, err)
	trans.Close()
}

func TestNewTMultiUDPClientTransportBadAddress(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{"not an address"}, "")
	assert.Nil(t, trans)
	assert.NotNil(t, err)
}

func TestTMultiUDPTransportOpen(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	assert.Nil(t, trans.Open())

	trans.Close()
}

func TestTMultiUDPTransportOpenFail(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	trans.transports[1].(*mockTransport).openError = fmt.Errorf("error")
	assert.NotNil(t, trans.Open())
	trans.Close()
}

func TestTMultiUDPTransportIsOpen(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	trans.transports[0].(*mockTransport).currentlyOpen = true
	trans.transports[1].(*mockTransport).currentlyOpen = true
	assert.True(t, trans.IsOpen())
	trans.Close()
}

func TestTMultiUDPTransportIsNotOpen(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	assert.False(t, trans.IsOpen())
	trans.Close()
}

func TestTMultiUDPTransportReadNotSupported(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	n, err := trans.Read([]byte{})
	assert.Equal(t, int(0), n)
	assert.NotNil(t, err)
	trans.Close()
}

func TestTMultiUDPTransportRemainingBytesNotSupported(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	n := trans.RemainingBytes()
	assert.Equal(t, uint64(0), n)
	trans.Close()
}

func TestTMultiUDPTransportWrite(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	trans.transports[0].(*mockTransport).onWrite = func(buf []byte) (int, error) {
		return len(buf), nil
	}
	trans.transports[1].(*mockTransport).onWrite = func(buf []byte) (int, error) {
		return len(buf), nil
	}

	b := []byte{1, 2, 3}
	n, err := trans.Write(b)
	assert.Equal(t, int(len(b)), n)
	assert.Nil(t, err)

	trans.Flush()
	trans.Close()
}

func TestTMultiUDPTransportWriteError(t *testing.T) {
	trans, err := NewTMultiUDPClientTransport([]string{}, "")
	assert.Nil(t, err)

	trans.transports = []thrift.TTransport{newMockTransport(), newMockTransport()}
	trans.transports[0].(*mockTransport).onWrite = func(buf []byte) (int, error) {
		return 0, fmt.Errorf("error")
	}
	trans.transports[1].(*mockTransport).onWrite = func(buf []byte) (int, error) {
		return len(buf), nil
	}

	b := []byte{1, 2, 3}
	n, err := trans.Write(b)
	assert.Equal(t, int(0), n)
	assert.NotNil(t, err)
	trans.Close()
}

type mockTransport struct {
	currentlyOpen    bool
	onRead           func(buf []byte) (int, error)
	onRemainingBytes func() uint64
	onWrite          func(buf []byte) (int, error)
	onFlush          func() error
	openError        error
	closeError       error
}

func newMockTransport() *mockTransport {
	return &mockTransport{}
}

func (m *mockTransport) Open() error {
	if m.openError != nil {
		return m.openError
	}
	m.currentlyOpen = true
	return nil
}

func (m *mockTransport) IsOpen() bool {
	return m.currentlyOpen
}

func (m *mockTransport) Close() error {
	if m.closeError != nil {
		return m.closeError
	}
	m.currentlyOpen = false
	return nil
}

func (m *mockTransport) Read(buf []byte) (int, error) {
	if m.onRead != nil {
		return m.onRead(buf)
	}
	return 0, fmt.Errorf("no mock Read implementation")
}

func (m *mockTransport) RemainingBytes() uint64 {
	if m.onRemainingBytes != nil {
		return m.onRemainingBytes()
	}
	return 0
}

func (m *mockTransport) Write(buf []byte) (int, error) {
	if m.onWrite != nil {
		return m.onWrite(buf)
	}
	return 0, fmt.Errorf("no mock Write implementation")
}

func (m *mockTransport) Flush() error {
	if m.onFlush != nil {
		return m.onFlush()
	}
	return fmt.Errorf("no mock Flush implementation")
}
