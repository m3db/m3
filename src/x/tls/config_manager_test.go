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
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type counterMock struct {
	incCallback func()
}

func (c *counterMock) Inc(_ int64) {
	c.incCallback()
}

func appendCA(filename string, certPool *x509.CertPool) error {
	certs, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read bundle error: %w", err)
	}
	if ok := certPool.AppendCertsFromPEM(certs); !ok {
		return fmt.Errorf("cannot append cert to cert pool")
	}
	return nil
}

func TestLoadCertPool(t *testing.T) {
	opts := NewOptions()
	cm := &configManager{
		options:  opts,
		certPool: x509.NewCertPool(),
	}
	expectedCertPool := x509.NewCertPool()

	opts = opts.SetCAFile("")
	cm.options = opts
	certPool, err := cm.loadCertPool()
	require.NoError(t, err)
	require.True(t, expectedCertPool.Equal(certPool))

	opts = opts.SetCAFile("testdata/1.crt")
	cm.options = opts
	certPool, err = cm.loadCertPool()
	require.NoError(t, err)
	err = appendCA("testdata/1.crt", expectedCertPool)
	require.NoError(t, err)
	require.True(t, expectedCertPool.Equal(certPool))
	require.True(t, expectedCertPool.Equal(cm.certPool))

	opts = opts.SetCAFile("testdata/2.crt")
	cm.options = opts
	certPool, err = cm.loadCertPool()
	require.NoError(t, err)
	err = appendCA("testdata/2.crt", expectedCertPool)
	require.NoError(t, err)
	require.True(t, expectedCertPool.Equal(certPool))
	require.True(t, expectedCertPool.Equal(cm.certPool))

	opts = opts.SetCAFile("testdata/3.crt")
	cm.options = opts
	certPool, err = cm.loadCertPool()
	require.Error(t, err)
	require.True(t, expectedCertPool.Equal(cm.certPool))

	opts = opts.SetCAFile("wrong/path")
	cm.options = opts
	certPool, err = cm.loadCertPool()
	require.Error(t, err)
}

func TestLoadX509KeyPair(t *testing.T) {
	opts := NewOptions()
	cm := &configManager{
		options: opts,
	}

	opts = opts.SetCertFile("").SetKeyFile("not empty")
	cm.options = opts
	certificates, err := cm.loadX509KeyPair()
	require.NoError(t, err)
	require.Len(t, certificates, 0)

	opts = opts.SetCertFile("not empty").SetKeyFile("")
	cm.options = opts
	certificates, err = cm.loadX509KeyPair()
	require.NoError(t, err)
	require.Len(t, certificates, 0)

	opts = opts.SetCertFile("wrong/path").SetKeyFile("wrong/path")
	cm.options = opts
	certificates, err = cm.loadX509KeyPair()
	require.Error(t, err)

	opts = opts.SetCertFile("testdata/1.crt").SetKeyFile("testdata/1.key")
	cm.options = opts
	certificates, err = cm.loadX509KeyPair()
	require.NoError(t, err)
	require.Len(t, certificates, 1)
}

func TestLoadTLSConfig(t *testing.T) {
	opts := NewOptions()
	cm := &configManager{
		options:  opts,
		certPool: x509.NewCertPool(),
		log:      zap.NewNop(),
	}

	opts = opts.SetCAFile("wrong/path")
	cm.options = opts
	tlsConfig, err := cm.loadTLSConfig()
	require.Error(t, err)

	opts = opts.SetCAFile("testdata/1.crt")
	opts = opts.SetCertFile("wrong/path").SetKeyFile("wrong/path")
	cm.options = opts
	tlsConfig, err = cm.loadTLSConfig()
	require.Error(t, err)

	opts = opts.
		SetCertFile("testdata/1.crt").
		SetKeyFile("testdata/1.key").
		SetMutualTLSEnabled(true).
		SetInsecureSkipVerify(true).
		SetServerName("server name")
	cm.options = opts
	tlsConfig, err = cm.loadTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, tlsConfig.RootCAs)
	require.NotNil(t, tlsConfig.ClientCAs)
	require.Len(t, tlsConfig.Certificates, 1)
	require.Equal(t, tls.RequireAndVerifyClientCert, tlsConfig.ClientAuth)
	require.True(t, tlsConfig.InsecureSkipVerify)
	require.Equal(t, "server name", tlsConfig.ServerName)
}

func TestUpdateCertificate(t *testing.T) {
	var waitCh = make(chan bool)
	var waitCalledCh = make(chan bool)
	var readConfigSuccess = 0
	successCounterMock := &counterMock{
		incCallback: func() { readConfigSuccess++ },
	}
	var readConfigErrors = 0
	errorCounterMock := &counterMock{
		incCallback: func() { readConfigErrors++ },
	}
	cmm := configManagerMetrics{
		getTLSConfigSuccess: successCounterMock,
		getTLSConfigErrors:  errorCounterMock,
	}
	originalSleepFn := sleepFn
	sleepFn = func(d time.Duration) {
		waitCalledCh <- true
		<-waitCh
	}
	defer func() { sleepFn = originalSleepFn }()

	instrumentOpts := instrument.NewOptions().SetLogger(zap.NewNop())
	opts := NewOptions().
		SetCertificatesTTL(0)
	cmInterface := NewConfigManager(opts, instrumentOpts)
	cm := cmInterface.(*configManager)
	cm.metrics = cmm
	require.Nil(t, cm.tlsConfig)
	require.Equal(t, 0, readConfigSuccess)
	require.Equal(t, 0, readConfigErrors)

	opts = opts.
		SetCertificatesTTL(time.Second).
		SetCAFile("testdata/1.crt").
		SetCertFile("testdata/1.crt").
		SetKeyFile("testdata/1.key").
		SetMutualTLSEnabled(true).
		SetInsecureSkipVerify(true).
		SetServerName("server name")
	cmInterface = NewConfigManager(opts, instrumentOpts)
	cm = cmInterface.(*configManager)
	cm.metrics = cmm
	<-waitCalledCh
	require.NotNil(t, cm.tlsConfig)
	require.Equal(t, 1, readConfigSuccess)
	require.Equal(t, 0, readConfigErrors)

	opts = opts.SetCAFile("wrong/path")
	cm.options = opts
	waitCh <- true
	<-waitCalledCh
	require.Equal(t, 1, readConfigSuccess)
	require.Equal(t, 1, readConfigErrors)
}
