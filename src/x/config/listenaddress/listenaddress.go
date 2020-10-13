// Copyright (c) 2018 Uber Technologies, Inc.
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

// Package listenaddress provides a configuration struct for resolving
// a listen address from YAML.
package listenaddress

const (
	defaultHostname = "0.0.0.0"
)

// Resolver is a type of port resolver
type Resolver string

// Configuration is the configuration for resolving a listen address.
type Configuration struct {
	// Value is the config specified listen address if using config port type.
	Value string `yaml:"value"`
}

// TODO: in fact, maybe just get rid of this entire package and then
// add local structs with the listen address string directly inline.
// e.g.
// type FooConfiguration struct {
//   ListenAddress string `yaml:"listenAddress"`
// }
