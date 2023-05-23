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

package integration

import (
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"

	"gopkg.in/yaml.v2"
)

// BaseConfigWithAuthEnabled encapsulate auth enabled config.
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

// BaseConfigWithAuthEnabledRefresh encapsulate auth enabled config refreshed.
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

// BaseConfigWithAuthDisabled encapsulate auth disabled config.
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

// BaseConfigWithOutbounds encapsulate auth config with valid outbounds.
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

// BaseConfigWithOutboundsrefreshed encapsulate auth config with valid outbounds refresh.
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

// CreateTestConfigYaml returns authconfig.
func CreateTestConfigYaml(cfg string) *config.AuthConfig {
	newSecrets := &config.AuthConfig{}
	err := yaml.Unmarshal([]byte(cfg), newSecrets)
	if err != nil {
		return nil
	}
	return newSecrets
}
