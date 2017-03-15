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

package m3

import (
	"testing"

	"github.com/uber-go/tally/m3/thriftudp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigSimple(t *testing.T) {
	c := Configuration{
		HostPort: "127.0.0.1:9052",
		Service:  "my-service",
		Env:      "test",
	}
	r, err := c.NewReporter()
	require.NoError(t, err)

	reporter := r.(*reporter)
	_, ok := reporter.client.Transport.(*thriftudp.TUDPTransport)
	assert.True(t, ok)
	assert.True(t, tagEquals(reporter.commonTags, "service", "my-service"))
	assert.True(t, tagEquals(reporter.commonTags, "env", "test"))
}

func TestConfigMulti(t *testing.T) {
	c := Configuration{
		HostPorts: []string{"127.0.0.1:9052", "127.0.0.1:9062"},
		Service:   "my-service",
		Env:       "test",
	}
	r, err := c.NewReporter()
	require.NoError(t, err)

	reporter := r.(*reporter)
	_, ok := reporter.client.Transport.(*thriftudp.TMultiUDPTransport)
	assert.True(t, ok)
	assert.True(t, tagEquals(reporter.commonTags, "service", "my-service"))
	assert.True(t, tagEquals(reporter.commonTags, "env", "test"))
}
