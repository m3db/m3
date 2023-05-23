package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInboundCredentials_Validate(t *testing.T) {
	tests := []struct {
		name     string
		userName string
		digest   string
		credtype CredentialType
		err      string
	}{
		{
			name:     "no error",
			userName: "abc",
			digest:   "xyz",
			credtype: ClientCredential,
		}, {
			name:   "username missing",
			digest: "xyz",
			err:    "username field is empty",
		}, {
			name:     "digest missing",
			userName: "xyz",
			credtype: ClientCredential,
			err:      "digest field is empty for inbound",
		}, {
			name:     "cred type missing",
			userName: "abc",
			digest:   "xyz",
			err:      "incorrect cred type field for inbound",
		}, {
			name:     "incorrect cred type",
			userName: "abc",
			digest:   "xyz",
			credtype: EtcdCredential,
			err:      "incorrect cred type field for inbound",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCreds := &InboundCredentials{Username: tt.userName, Digest: tt.digest, Type: tt.credtype}
			err := newCreds.Validate()
			if tt.err != "" {
				assert.Contains(t, err.Error(), tt.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestOutboundCredentials_Validate(t *testing.T) {
	tests := []struct {
		name     string
		userName string
		password string
		zone     string
		credtype CredentialType
		err      string
	}{
		{
			name:     "no error",
			userName: "abc",
			password: "xyz",
			zone:     "foo",
			credtype: EtcdCredential,
		}, {
			name:     "username missing",
			password: "xyz",
			zone:     "foo",
			err:      "username field is empty",
		}, {
			name:     "password missing",
			userName: "xyz",
			zone:     "foo",
			credtype: EtcdCredential,
			err:      "password field is empty",
		}, {
			name:     "zone missing",
			userName: "abc",
			password: "xyz",
			credtype: PeerCredential,
			err:      "zone field is not set",
		}, {
			name:     "zone missing for dbnode - etcd",
			userName: "abc",
			password: "xyz",
			credtype: EtcdCredential,
			err:      "zone field is not set",
		}, {
			name:     "cred type missing",
			userName: "abc",
			password: "xyz",
			zone:     "foo",
			err:      "incorrect cred type field for outbound",
		}, {
			name:     "incorrect cred type ",
			userName: "abc",
			password: "xyz",
			zone:     "foo",
			credtype: ClientCredential,
			err:      "incorrect cred type field for outbound",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCreds := &OutboundCredentials{Username: tt.userName, Password: tt.password, Zone: tt.zone, Type: tt.credtype}
			err := newCreds.Validate()
			if tt.err != "" {
				assert.Contains(t, err.Error(), tt.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
