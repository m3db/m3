// Copyright (c) 2019 Uber Technologies, Inc.
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

package xhttp

import (
	"net"
	"net/http"
	"net/url"
	"time"
)

// ProxyFunc is a type alias for the proxy function used by the standard
// library's http.Transport struct.
type ProxyFunc func(*http.Request) (*url.URL, error)

// HTTPClientOptions specify HTTP Client options.
type HTTPClientOptions struct {
	RequestTimeout     time.Duration `yaml:"requestTimeout"`
	ConnectTimeout     time.Duration `yaml:"connectTimeout"`
	KeepAlive          time.Duration `yaml:"keepAlive"`
	IdleConnTimeout    time.Duration `yaml:"idleConnTimeout"`
	MaxIdleConns       int           `yaml:"maxIdleConns"`
	DisableCompression bool          `yaml:"disableCompression"`
	Proxy              ProxyFunc     `yaml:"proxy"`
}

// NewHTTPClient constructs a new HTTP Client.
func NewHTTPClient(o HTTPClientOptions) *http.Client {
	// Before we added the Proxy option, we unconditionally set the field in
	// the http.Transport we construct below to http.ProxyFromEnvironment. To
	// keep that behavior unchanged now that we added the field, we need to
	// set the option if it is nil.
	if o.Proxy == nil {
		o.Proxy = http.ProxyFromEnvironment
	}

	return &http.Client{
		Timeout: o.RequestTimeout,
		Transport: &http.Transport{
			Proxy: o.Proxy,
			Dial: (&net.Dialer{
				Timeout:   o.ConnectTimeout,
				KeepAlive: o.KeepAlive,
				DualStack: true,
			}).Dial,
			TLSHandshakeTimeout:   o.ConnectTimeout,
			ExpectContinueTimeout: o.ConnectTimeout,
			MaxIdleConns:          o.MaxIdleConns,
			MaxIdleConnsPerHost:   o.MaxIdleConns,
			DisableCompression:    o.DisableCompression,
		},
	}
}

// DefaultHTTPClientOptions returns default options.
func DefaultHTTPClientOptions() HTTPClientOptions {
	return HTTPClientOptions{
		RequestTimeout:  60 * time.Second,
		ConnectTimeout:  5 * time.Second,
		KeepAlive:       60 * time.Second,
		IdleConnTimeout: 60 * time.Second,
		MaxIdleConns:    100,
		// DisableCompression is true by default since we have seen
		// a large amount of overhead with compression.
		DisableCompression: true,
		Proxy:              ProxyFunc(http.ProxyFromEnvironment),
	}
}
