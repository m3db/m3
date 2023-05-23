package client

import (
	"os"
	"testing"

	"github.com/m3db/m3/src/dbnode/auth"
	xconfig "github.com/m3db/m3/src/x/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigurationWithAuthClientProvider(t *testing.T) {
	clientCfg := `
  config:
    service:
      env: default_env
      zone: embedded
      service: m3db
      auth:
          enabled: true
          username: user_db
          password: pass_db
      etcdClusters:
        - zone: embedded
          endpoints:
            - etcd:2379
            - etcd:456
          auth:
            enabled: true
            username: user_etcd
            password: pass_etcd
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	outboundErr := setupAndLoadCfg(t, clientCfg)
	require.NoError(t, outboundErr)
}

func TestConfigurationWithIncorrectDbAuth(t *testing.T) {
	clientCfg := `
  config:
    service:
      env: default_env
      zone: embedded
      service: m3db
      auth:
          enabled: true
          password: pass_db
      etcdClusters:
        - zone: embedded
          endpoints:
            - etcd:2379
            - etcd:456
          auth:
            enabled: true
            username: user_etcd
            password: pass_etcd
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	outboundErr := setupAndLoadCfg(t, clientCfg)
	require.Error(t, outboundErr)
}

func TestConfigurationWithIncorrectEtcdAuth(t *testing.T) {
	clientCfg := `
  config:
    service:
      env: default_env
      zone: embedded
      service: m3db
      auth:
          enabled: true
          username: user_db
          password: pass_db
      etcdClusters:
        - zone: embedded
          endpoints:
            - etcd:2379
            - etcd:456
          auth:
            enabled: true
            password: pass_etcd
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	outboundErr := setupAndLoadCfg(t, clientCfg)
	require.Error(t, outboundErr)
}

func TestConfigurationNoAuth(t *testing.T) {
	clientCfg := `
  config:
    service:
      env: default_env
      zone: embedded
      service: m3db
      auth:
          enabled: false
          username: user_db
          password: pass_db
      etcdClusters:
        - zone: embedded
          endpoints:
            - etcd:2379
            - etcd:456
          auth:
            enabled: false
            password: pass_etcd
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	outboundErr := setupAndLoadCfg(t, clientCfg)
	require.NoError(t, outboundErr)
}

func setupAndLoadCfg(t *testing.T, config string) error {
	fd, err := os.CreateTemp("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.WriteString(config)
	require.NoError(t, err)

	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	defer outboundAuthCleanup()
	return PopulateClientOutboundAuthConfig(cfg.EnvironmentConfig.Services)
}
func outboundAuthCleanup() {
	auth.OutboundAuth = &auth.Outbound{}
}
