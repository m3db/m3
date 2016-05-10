// Copyright (c) 2015 Uber Technologies, Inc.

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

package mockhyperbahn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/hyperbahn"
	"github.com/uber/tchannel-go/testutils/mockhyperbahn"
)

var config = struct {
	hyperbahnConfig hyperbahn.Configuration
}{}

// setupServer is the application code we are attempting to test.
func setupServer() (*hyperbahn.Client, error) {
	ch, err := tchannel.NewChannel("myservice", nil)
	if err != nil {
		return nil, err
	}

	if err := ch.ListenAndServe("127.0.0.1:0"); err != nil {
		return nil, err
	}

	client, err := hyperbahn.NewClient(ch, config.hyperbahnConfig, nil)
	if err != nil {
		return nil, err
	}

	return client, client.Advertise()
}

func TestMockHyperbahn(t *testing.T) {
	mh, err := mockhyperbahn.New()
	require.NoError(t, err, "mock hyperbahn failed")
	defer mh.Close()

	config.hyperbahnConfig = mh.Configuration()
	_, err = setupServer()
	require.NoError(t, err, "setupServer failed")
	assert.Equal(t, []string{"myservice"}, mh.GetAdvertised())
}

func TestMockDiscovery(t *testing.T) {
	mh, err := mockhyperbahn.New()
	require.NoError(t, err, "mock hyperbahn failed")
	defer mh.Close()

	peers := []string{
		"1.3.5.7:1456",
		"255.255.255.255:25",
	}
	mh.SetDiscoverResult("discover-svc", peers)

	config.hyperbahnConfig = mh.Configuration()
	client, err := setupServer()
	require.NoError(t, err, "setupServer failed")

	gotPeers, err := client.Discover("discover-svc")
	require.NoError(t, err, "Discover failed")
	assert.Equal(t, peers, gotPeers, "Discover returned invalid peers")
}
