// Copyright (c) 2023 Uber Technologies, Inc.
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

package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/thrift"
)

func TestWrapThriftContextWithCreds(t *testing.T) {
	outbound := &Outbound{
		peerCredentials: []OutboundCredentials{
			{
				Username: "test",
				Password: "password",
				Zone:     "test-zone",
			},
		},
	}
	tctx, _ := thrift.NewContext(time.Minute)
	tctx = thrift.WithHeaders(tctx, map[string]string{
		"key1": "value1",
		"key2": "value2",
	})

	wrappedTctx := outbound.WrapThriftContextWithPeerCreds(tctx, "test-zone")
	assert.Equal(t, "test", wrappedTctx.Headers()["username"], "Expected username in wrapped context headers")
	assert.Equal(t, "password", wrappedTctx.Headers()["password"], "Expected password in wrapped context headers")

	assert.Equal(t, "", wrappedTctx.Headers()["key1"], "Expected initial keys must be flushed")
	assert.Equal(t, "", wrappedTctx.Headers()["key2"], "Expected initial keys must be flushed")
}

func TestWrapThriftContextWithOutboundPeerCredsNil(t *testing.T) {
	outbound := &Outbound{}
	tctx, _ := thrift.NewContext(time.Minute)
	tctx = thrift.WithHeaders(tctx, map[string]string{
		"key1": "value1",
		"key2": "value2",
	})

	wrappedTctx := outbound.WrapThriftContextWithPeerCreds(tctx, "test-zone")
	assert.Equal(t, "", wrappedTctx.Headers()["username"], "Expected no username in wrapped context headers")
	assert.Equal(t, "", wrappedTctx.Headers()["password"], "Expected no password in wrapped context headers")

	assert.Equal(t, "value1", wrappedTctx.Headers()["key1"], "Expected initial keys must be present")
	assert.Equal(t, "value2", wrappedTctx.Headers()["key2"], "Expected initial keys must be present")
}

func TestFetchOutboundEtcdCredentials(t *testing.T) {
	outbound := &Outbound{
		etcdCredentials: []OutboundCredentials{
			{
				Username: "test",
				Password: "password",
				Zone:     "test-zone",
			},
		},
	}
	creds := outbound.FetchOutboundEtcdCredentials("test-zone")
	assert.Equal(t, &outbound.etcdCredentials[0], creds, "Expected no error with matching zone creds")
}

func TestWrapThriftContextWithOutboundEtcdCredsNil(t *testing.T) {
	outbound := &Outbound{}
	creds := outbound.FetchOutboundEtcdCredentials("test-zone")
	assert.Nil(t, creds, "Expected no credential")
}

func TestFetchOutboundEtcdCredentialsIncorrectZone(t *testing.T) {
	outbound := &Outbound{
		etcdCredentials: []OutboundCredentials{
			{
				Username: "test",
				Password: "password",
				Zone:     "test-zone",
			},
		},
	}
	creds := outbound.FetchOutboundEtcdCredentials("foo")
	assert.Nil(t, creds, "Expected response obj to be nil")
}
