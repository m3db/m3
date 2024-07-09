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

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const getConfigMetricName = "get-tls-config"

var sleepFn = time.Sleep

type ConfigManager interface {
	TLSConfig() (*tls.Config, error)
	ServerMode() ServerMode
	ClientEnabled() bool
}

type configManager struct {
	sync.RWMutex

	log       *zap.Logger
	metrics   configManagerMetrics
	options   Options
	certPool  *x509.CertPool
	tlsConfig *tls.Config
}

type configManagerMetrics struct {
	getTLSConfigSuccess tally.Counter
	getTLSConfigErrors  tally.Counter
}

func newConfigManagerMetrics(scope tally.Scope) configManagerMetrics {
	return configManagerMetrics{
		getTLSConfigSuccess: scope.Tagged(map[string]string{"success": "true"}).Counter(getConfigMetricName),
		getTLSConfigErrors:  scope.Tagged(map[string]string{"success": "false"}).Counter(getConfigMetricName),
	}
}

func NewConfigManager(opts Options, instrumentOpts instrument.Options) ConfigManager {
	scope := instrumentOpts.MetricsScope()
	c := &configManager{
		log:      instrumentOpts.Logger(),
		metrics:  newConfigManagerMetrics(scope),
		options:  opts,
		certPool: x509.NewCertPool(),
	}
	go c.updateCertificates()
	return c
}

func (c *configManager) updateCertificates() {
	if c.options.CertificatesTTL() == 0 {
		return
	}
	for {
		tlsConfig, err := c.loadTLSConfig()
		if err != nil {
			c.metrics.getTLSConfigErrors.Inc(1)
			c.log.Error("get tls config error", zap.Error(err))
			sleepFn(c.options.CertificatesTTL())
			continue
		}
		c.Lock()
		c.tlsConfig = tlsConfig
		c.Unlock()
		c.metrics.getTLSConfigSuccess.Inc(1)
		sleepFn(c.options.CertificatesTTL())
	}
}

func (c *configManager) loadCertPool() (*x509.CertPool, error) {
	if c.options.CAFile() != "" {
		certs, err := os.ReadFile(c.options.CAFile())
		if err != nil {
			return nil, fmt.Errorf("read bundle error: %w", err)
		}
		if ok := c.certPool.AppendCertsFromPEM(certs); !ok {
			return nil, fmt.Errorf("cannot append cert to cert pool")
		}
	}
	return c.certPool, nil
}

func (c *configManager) loadX509KeyPair() ([]tls.Certificate, error) {
	if c.options.CertFile() == "" || c.options.KeyFile() == "" {
		return []tls.Certificate{}, nil
	}
	cert, err := tls.LoadX509KeyPair(c.options.CertFile(), c.options.KeyFile())
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair error: %w", err)
	}
	return []tls.Certificate{cert}, nil
}

func (c *configManager) loadTLSConfig() (*tls.Config, error) {
	certPool, err := c.loadCertPool()
	if err != nil {
		return nil, fmt.Errorf("load cert pool failed: %w", err)
	}
	certificate, err := c.loadX509KeyPair()
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair failed: %w", err)
	}
	clientAuthType := tls.NoClientCert
	if c.options.MutualTLSEnabled() {
		clientAuthType = tls.RequireAndVerifyClientCert
	}
	return &tls.Config{
		RootCAs:            certPool,
		ClientCAs:          certPool,
		Certificates:       certificate,
		ClientAuth:         clientAuthType,
		InsecureSkipVerify: c.options.InsecureSkipVerify(),
		ServerName:         c.options.ServerName(),
	}, nil
}

func (c *configManager) TLSConfig() (*tls.Config, error) {
	if c.options.CertificatesTTL() == 0 || c.tlsConfig == nil {
		return c.loadTLSConfig()
	}
	c.RLock()
	defer c.RUnlock()
	return c.tlsConfig, nil
}

func (c *configManager) ServerMode() ServerMode {
	return c.options.ServerMode()
}

func (c *configManager) ClientEnabled() bool {
	return c.options.ClientEnabled()
}
