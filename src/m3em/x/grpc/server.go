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

package xgrpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	defaultGrpcMaxConcurrentStreams = 16384
)

func options(tCred credentials.TransportCredentials) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(defaultGrpcMaxConcurrentStreams),
	}

	if tCred != nil {
		opts = append(opts, grpc.Creds(tCred))
	}

	return opts
}

// NewServer returns a new grpc Server with provided options
func NewServer(tc credentials.TransportCredentials) *grpc.Server {
	return grpc.NewServer(options(tc)...)
}

// NewServerCredentials returns Server Credentials
func NewServerCredentials(
	caCertPEMBlock []byte,
	serverCertPEMBlock []byte,
	serverKeyPEMBlock []byte,
) (credentials.TransportCredentials, error) {
	certPool, certificate, err := newCert(caCertPEMBlock, serverCertPEMBlock, serverKeyPEMBlock)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    certPool,
	}
	return credentials.NewTLS(tlsConfig), nil
}

// NewClientCredentials returns Client Credentials
func NewClientCredentials(
	serverName string,
	caCertPEMBlock []byte,
	clientCertPEMBlock []byte,
	clientKeyPEMBlock []byte,
) (credentials.TransportCredentials, error) {
	certPool, certificate, err := newCert(caCertPEMBlock, clientCertPEMBlock, clientKeyPEMBlock)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		ServerName:   serverName,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	}
	return credentials.NewTLS(tlsConfig), nil
}

func newCert(
	caCertPEMBlock []byte,
	certPEMBlock []byte,
	keyPEMBlock []byte,
) (*x509.CertPool, tls.Certificate, error) {
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(caCertPEMBlock)
	if !ok {
		return nil, tls.Certificate{}, fmt.Errorf("unable to append CA Cert")
	}

	certificate, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, tls.Certificate{}, err
	}

	return certPool, certificate, nil
}
