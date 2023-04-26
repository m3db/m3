package auth

import (
	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/thrift"
	"testing"
	"time"
)

func TestWrapThriftContextWithCreds(t *testing.T) {
	outbound := &Outbound{
		peerCredentials: []Credentials{
			{
				OutboundCredentials: OutboundCredentials{
					Username: "test",
					Password: "password",
					Zone:     "test-zone",
				},
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
		etcdCredentials: []Credentials{
			{
				OutboundCredentials: OutboundCredentials{
					Username: "test",
					Password: "password",
					Zone:     "test-zone",
				},
			},
		},
	}
	creds := outbound.FetchOutboundEtcdCredentials("test-zone")
	assert.Equal(t, &outbound.etcdCredentials[0].OutboundCredentials, creds, "Expected no error with matching zone creds")
}

func TestWrapThriftContextWithOutboundEtcdCredsNil(t *testing.T) {
	outbound := &Outbound{}
	creds := outbound.FetchOutboundEtcdCredentials("test-zone")
	assert.Nil(t, creds, "Expected no credential")
}

func TestFetchOutboundEtcdCredentialsIncorrectZone(t *testing.T) {
	outbound := &Outbound{
		etcdCredentials: []Credentials{
			{
				OutboundCredentials: OutboundCredentials{
					Username: "test",
					Password: "password",
					Zone:     "test-zone",
				},
			},
		},
	}
	creds := outbound.FetchOutboundEtcdCredentials("foo")
	assert.Nil(t, creds, "Expected response obj to be nil")
}
