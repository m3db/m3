package config

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
	"io"
	"os"
	"testing"
)

const testBaseAuthConfig = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc
          username: m3_bcd
          password: m3_xyz
      - service:
          zone: m3_abc1
          username: m3_bcd1
          password: m3_xyz1

  etcd:
    services:
      - service:
          zone: etcd_abc
          username: etcd_bcd
          password: etcd_xyz
      - service:
          zone: etcd_abc1
          username: etcd_bcd1
          password: etcd_xyz1

inbounds:
  m3db:
    mode: NONE
    credentials:
      - username: user
        digest: digest1
      - username: user2
        digest: digest2
`

const missingOutbounds = `

inbounds:
  m3db:
    mode: NONE
    credentials:
      - username: user
        password: digest
      - username: user2
        password: digest2
`

const missingM3dbConfigOutbounds = `
outbounds:
  etcd:
    services:
      - service:
          zone: etcd_abc
          username: etcd_bcd
          password: etcd_xyz
      - service:
          zone: etcd_abc1
          username: etcd_bcd1
          password: etcd_xyz1

inbounds:
  m3db:
    mode: NONE
    credentials:
      - username: user
        digest: digest1
      - username: user2
        digest: digest2
`

const missingETCDConfigOutbounds = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc
          username: m3_bcd
          password: m3_xyz
      - service:
          zone: m3_abc1
          username: m3_bcd1
          password: m3_xyz1

inbounds:
  m3db:
    mode: NONE
    credentials:
      - username: user
        digest: digest1
      - username: user2
        digest: digest2
`

func TestAuthConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  string
		err  error
	}{
		{
			name: "correct secrets file",
			cfg:  testBaseAuthConfig,
			err:  nil,
		}, {
			name: "secrets file missing outbounds",
			cfg:  missingOutbounds,
			err:  errors.New("outbound creds are not present"),
		}, {
			name: "secrets file missing m3db outbounds",
			cfg:  missingM3dbConfigOutbounds,
			err:  errors.New("incomplete outbound creds, m3db node creds not provided"),
		}, {
			name: "secrets file missing etcd outbounds",
			cfg:  missingETCDConfigOutbounds,
			err:  errors.New("incomplete outbound creds, etcd node creds not provided"),
		}, {
			name: "secrets file contains incomplete creds",
			cfg:  missingETCDConfigOutbounds,
			err:  errors.New("incomplete creds, password not provided"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fd, crtErr := os.CreateTemp("", "secrets.yaml")
			require.NoError(t, crtErr)
			defer func() {
				assert.NoError(t, fd.Close())
				assert.NoError(t, os.Remove(fd.Name()))
			}()

			_, wrtErr := fd.Write([]byte(tt.cfg))
			require.NoError(t, wrtErr)

			f, opnErr := os.Open(fd.Name())
			require.NoError(t, opnErr)

			all, rdErr := io.ReadAll(f)
			require.NoError(t, rdErr)

			newSecrets := &AuthConfig{}
			unmarErr := yaml.Unmarshal(all, newSecrets)
			require.NoError(t, unmarErr)

			err := newSecrets.Validate()
			if tt.err != nil {
				assert.Error(t, err)
			}
		})
	}
}
