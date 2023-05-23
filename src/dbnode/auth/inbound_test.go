package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/thrift"
)

func TestInbound_ValidateCredentials(t *testing.T) {
	t.Run("valid credentials no auth mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupNoAuthMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc",
			Digest:   "bcd",
			Type:     ClientCredential,
		})
		assert.NoError(t, err)
	})

	t.Run("invalid credentials no auth mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupNoAuthMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc1",
			Digest:   "bcd1",
			Type:     ClientCredential,
		})
		assert.NoError(t, err)
	})

	t.Run("valid credentials shadow mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupShadowMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc",
			Digest:   "bcd",
			Type:     ClientCredential,
		})
		assert.NoError(t, err)
	})

	t.Run("invalid credentials shadow mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupShadowMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc1",
			Digest:   "bcd1",
			Type:     ClientCredential,
		})
		assert.NoError(t, err)
	})

	t.Run("valid credentials enforced mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "foo",
			Digest:   "zoo",
			Type:     ClientCredential,
		})
		assert.NoError(t, err)
	})

	t.Run("invalid credentials enforced mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc1",
			Digest:   "bcd1",
			Type:     ClientCredential,
		})
		assert.Error(t, err)
	})

	t.Run("unknown credential type for inbound", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc1",
			Digest:   "bcd1",
			Type:     Unknown,
		})
		assert.Error(t, err)
	})

	t.Run("non client credential type for inbound", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		err := inboundAuth.ValidateCredentials(InboundCredentials{
			Username: "abc1",
			Digest:   "bcd1",
			Type:     PeerCredential,
		})
		assert.Error(t, err)
	})
}

func TestInbound_ValidateCredentialsFromThriftContext(t *testing.T) {
	t.Run("valid credentials no auth mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupNoAuthMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc", AuthPassword: "bcd"},
		)
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.NoError(t, err)
	})

	t.Run("invalid credentials no auth mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupNoAuthMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc1", AuthPassword: "bcd1"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.NoError(t, err)
	})

	t.Run("valid credentials shadow mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupShadowMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc", AuthPassword: "bcd"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.NoError(t, err)
	})

	t.Run("invalid credentials shadow mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupShadowMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc1", AuthPassword: "bcd1"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.NoError(t, err)
	})

	t.Run("valid credentials enforced mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc", AuthPassword: "bcd"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.NoError(t, err)
	})

	t.Run("invalid credentials enforced mode", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc1", AuthPassword: "bcd1"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.Error(t, err)
	})

	t.Run("unknown credential type for inbound", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc", AuthPassword: "bcd"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, Unknown)
		assert.Error(t, err)
	})

	t.Run("non client credential type for inbound", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{AuthUsername: "abc", AuthPassword: "bcd"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, PeerCredential)
		assert.Error(t, err)
	})

	t.Run("non client credential type for inbound", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		thriftCtx := createThriftContextWithHeaders(
			context.Background(),
			map[string]string{"some_u": "abc", "some_p": "bcd"})
		err := inboundAuth.ValidateCredentialsFromThriftContext(thriftCtx, ClientCredential)
		assert.Error(t, err)
	})
}

func TestInbound_MatchCredentials(t *testing.T) {
	t.Run("matching credentials", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		c1 := &InboundCredentials{
			Username: "abc",
			Digest:   "bcd",
			Type:     ClientCredential,
		}

		c2 := &InboundCredentials{
			Username: "abc",
			Digest:   "bcd",
			Type:     ClientCredential,
		}

		ok := inboundAuth.MatchCredentials(*c1, *c2)
		assert.True(t, ok)
	})

	t.Run("non matching credentials", func(t *testing.T) {
		inboundAuth := inboundAuthSetupEnabledMode()
		c1 := &InboundCredentials{
			Username: "abc",
			Digest:   "bcd",
			Type:     ClientCredential,
		}

		c2 := &InboundCredentials{
			Username: "abc1",
			Digest:   "bcd",
			Type:     ClientCredential,
		}

		ok := inboundAuth.MatchCredentials(*c1, *c2)
		assert.False(t, ok)
	})
}

func inboundAuthSetupNoAuthMode() *Inbound {
	return &Inbound{
		clientCredentials: []InboundCredentials{
			{

				Username: "abc",
				Digest:   "bcd",
				Type:     ClientCredential,
			}, {

				Username: "foo",
				Digest:   "zoo",
				Type:     ClientCredential,
			},
		},
		authMode: AuthModeNoAuth,
	}
}

func inboundAuthSetupShadowMode() *Inbound {
	return &Inbound{
		clientCredentials: []InboundCredentials{
			{
				Username: "abc",
				Digest:   "bcd",
				Type:     ClientCredential,
			}, {

				Username: "foo",
				Digest:   "zoo",
				Type:     ClientCredential,
			},
		},
		authMode: AuthModeShadow,
	}
}

func inboundAuthSetupEnabledMode() *Inbound {
	return &Inbound{
		clientCredentials: []InboundCredentials{
			{

				Username: "abc",
				Digest:   "bcd",
				Type:     ClientCredential,
			}, {

				Username: "foo",
				Digest:   "zoo",
				Type:     ClientCredential,
			},
		},
		authMode: AuthModeEnforced,
	}
}

func createThriftContextWithHeaders(ctx context.Context, headers map[string]string) thrift.Context {
	return thrift.WithHeaders(ctx, headers)
}
