package integration

import (
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	yaml "gopkg.in/yaml.v2"
)

const BaseConfigWithAuthEnabled = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc11
          username: m3_bcd11
          password: m3_xyz11
      - service:
          zone: m3_abc1
          username: m3_bcd21
          password: m3_xyz21

  etcd:
    services:
      - service:
          zone: etcd_abc11
          username: etcd_bcd11
          password: etcd_xyz11
      - service:
          zone: etcd_abc1
          username: etcd_bcd21
          password: etcd_xyz21

inbounds:
  m3db:
    mode: ENABLED
    credentials:
      - username: user
        digest: digest11
      - username: user2
        digest: digest21
`

const BaseConfigWithAuthEnabledRefresh = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc2
          username: m3_bcd2
          password: m3_xyz2
      - service:
          zone: m3_abc22
          username: m3_bcd22
          password: m3_xyz22

  etcd:
    services:
      - service:
          zone: etcd_abc12
          username: etcd_bcd12
          password: etcd_xyz12
      - service:
          zone: etcd_abc22
          username: etcd_bcd22
          password: etcd_xyz22

inbounds:
  m3db:
    mode: ENABLED
    credentials:
      - username: user12
        digest: digest12
      - username: user22
        digest: digest22
`
const BaseConfigWithAuthDisabled = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc11
          username: m3_bcd11
          password: m3_xyz11
      - service:
          zone: m3_abc1
          username: m3_bcd21
          password: m3_xyz21

  etcd:
    services:
      - service:
          zone: etcd_abc11
          username: etcd_bcd11
          password: etcd_xyz11
      - service:
          zone: etcd_abc1
          username: etcd_bcd21
          password: etcd_xyz21

inbounds:
  m3db:
    mode: NONE
    credentials:
      - username: user
        digest: digest11
      - username: user2
        digest: digest21
`

const BaseConfigWithOutbounds = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc11
          username: m3_bcd11
          password: m3_xyz11
      - service:
          zone: m3_abc1
          username: m3_bcd21
          password: m3_xyz21

  etcd:
    services:
      - service:
          zone: etcd_abc11
          username: etcd_bcd11
          password: etcd_xyz11
      - service:
          zone: etcd_abc1
          username: etcd_bcd21
          password: etcd_xyz21

`

const BaseConfigWithOutboundsrefreshed = `
outbounds:
  m3db:
    services:
      - service:
          zone: m3_abc11
          username: m3_bcd12
          password: m3_xyz12
      - service:
          zone: m3_abc1
          username: m3_bcd22
          password: m3_xyz22

  etcd:
    services:
      - service:
          zone: etcd_abc11
          username: etcd_bcd12
          password: etcd_xyz12
      - service:
          zone: etcd_abc1
          username: etcd_bcd22
          password: etcd_xyz22

`

func CreateTestConfigYaml(cfg string) *config.AuthConfig {
	newSecrets := &config.AuthConfig{}
	yaml.Unmarshal([]byte(cfg), newSecrets)
	return newSecrets
}
