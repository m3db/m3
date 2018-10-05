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

package agentmain

import (
	"io/ioutil"

	xgrpc "github.com/m3db/m3/src/m3em/x/grpc"

	tallym3 "github.com/uber-go/tally/m3"
	"google.golang.org/grpc/credentials"
)

// Configuration is a collection of knobs to configure m3em_agent processes
type Configuration struct {
	Server  ServerConfiguration  `yaml:"server"`
	Metrics MetricsConfiguration `yaml:"metrics"`
	Agent   AgentConfiguration   `yaml:"agent"`
}

// AgentConfiguration is a collection of knobs to configure agents
type AgentConfiguration struct {
	WorkingDir  string            `yaml:"workingDir" validate:"nonzero"`
	StartupCmds []ExecCommand     `yaml:"startupCmds"`
	ReleaseCmds []ExecCommand     `yaml:"releaseCmds"`
	TestEnvVars map[string]string `yaml:"testEnvVars"`
}

// ExecCommand is an executable command
type ExecCommand struct {
	Path string   `yaml:"path" validate:"nonzero"`
	Args []string `yaml:"args"`
}

// MetricsConfiguration is a collection of knobs to configure metrics collection
type MetricsConfiguration struct {
	Prefix     string                `yaml:"prefix"`
	SampleRate float64               `yaml:"sampleRate" validate:"min=0.01,max=1.0"`
	M3         tallym3.Configuration `yaml:"m3"         validate:"nonzero"`
}

// ServerConfiguration is a collection of knobs to control grpc server configuration
type ServerConfiguration struct {
	ListenAddress string            `yaml:"listenAddress" validate:"nonzero"`
	DebugAddress  string            `yaml:"debugAddress"  validate:"nonzero"`
	TLS           *TLSConfiguration `yaml:"tls"`
}

// TLSConfiguration are the resources required for TLS Communication
type TLSConfiguration struct {
	CACrtPath     string `yaml:"caCrt" validate:"nonzero"`
	ServerCrtPath string `yaml:"serverCrt" validate:"nonzero"`
	ServerKeyPath string `yaml:"serverKey" validate:"nonzero"`
}

// Credentials returns the TransportCredentials corresponding to the provided struct
func (t TLSConfiguration) Credentials() (credentials.TransportCredentials, error) {
	caCrt, err := ioutil.ReadFile(t.CACrtPath)
	if err != nil {
		return nil, err
	}

	serverCrt, err := ioutil.ReadFile(t.ServerCrtPath)
	if err != nil {
		return nil, err
	}

	serverKey, err := ioutil.ReadFile(t.ServerKeyPath)
	if err != nil {
		return nil, err
	}

	return xgrpc.NewServerCredentials(caCrt, serverCrt, serverKey)
}
