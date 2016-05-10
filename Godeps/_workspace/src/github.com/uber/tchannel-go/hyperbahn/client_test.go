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

package hyperbahn

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/testutils"
)

func getPeers(ch *tchannel.Channel) []string {
	var peers []string
	for k := range ch.Peers().Copy() {
		peers = append(peers, k)
	}
	sort.Strings(peers)
	return peers
}

func TestParseConfiguration(t *testing.T) {
	peers1 := []string{"1.1.1.1:1", "2.2.2.2:2"}
	peers2 := []string{"3.3.3.3:3", "4.4.4.4:4"}
	invalidPeer1 := []string{"2:2:2:2"}
	invalidPeer2 := []string{"2.2.2.2"}
	peersFile2 := `["3.3.3.3:3", "4.4.4.4:4"]`

	tests := []struct {
		name      string
		peersArg  []string
		peersFile string
		wantPeers []string
		wantErr   bool
	}{
		{
			name:    "no peers",
			wantErr: true,
		},
		{
			name:      "no peer list",
			peersArg:  peers1,
			wantPeers: peers1,
		},
		{
			name:     "invalid peers invalid format",
			peersArg: invalidPeer1,
			wantErr:  true,
		},
		{
			name:     "invalid peers no port",
			peersArg: invalidPeer2,
			wantErr:  true,
		},
		{
			name:      "peer file",
			peersFile: peersFile2,
			wantPeers: peers2,
		},
		{
			name:      "peer file overrides args",
			peersArg:  peers1,
			peersFile: peersFile2,
			wantPeers: peers2,
		},
	}

	for _, tt := range tests {
		peerFile := ""
		if tt.peersFile != "" {
			f, err := ioutil.TempFile("", "hosts")
			if !assert.NoError(t, err, "%v: TempFile failed", tt.name) {
				continue
			}
			defer os.Remove(f.Name())

			_, err = f.WriteString(tt.peersFile)
			assert.NoError(t, err, "%v: write peer file failed", tt.name)

			assert.NoError(t, err, "%v: write peer file failed", tt.name)
			assert.NoError(t, f.Close(), "%v: close peer file failed", tt.name)
			peerFile = f.Name()
		}

		config := Configuration{InitialNodes: tt.peersArg, InitialNodesFile: peerFile}
		ch := testutils.NewClient(t, nil)
		defer ch.Close()

		_, err := NewClient(ch, config, nil)
		if tt.wantErr {
			assert.Error(t, err, "%v: NewClient expected to fail")
			continue
		}
		if !assert.NoError(t, err, "%v: hyperbahn.NewClient failed", tt.name) {
			continue
		}

		assert.Equal(t, tt.wantPeers, getPeers(ch), "%v: got unexpected peers", tt.name)
	}
}
