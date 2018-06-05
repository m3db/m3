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

package config

import (
	"fmt"
	"testing"

	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	bfs "github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/peers"

	"github.com/stretchr/testify/require"
)

var (
	peersBs     = peers.PeersBootstrapperName
	fsBs        = bfs.FileSystemBootstrapperName
	commitLogBs = commitlog.CommitLogBootstrapperName
	noOpAllBs   = bootstrapper.NoOpAllBootstrapperName
	noOpNoneBs  = bootstrapper.NoOpNoneBootstrapperName
)

func TestValidateBootstrappersOrder(t *testing.T) {
	tests := []struct {
		valid         bool
		bootstrappers []string
	}{
		{true, []string{fsBs, peersBs, commitLogBs, noOpNoneBs}},
		{true, []string{fsBs, commitLogBs, noOpNoneBs}},
		{true, []string{commitLogBs, noOpNoneBs}},
		{true, []string{fsBs, noOpNoneBs}},
		{true, []string{fsBs}},
		{true, []string{fsBs, peersBs}},
		{true, []string{peersBs}},
		{true, []string{peersBs, commitLogBs}},
		{true, []string{fsBs, commitLogBs}},
		{true, []string{noOpNoneBs}},
		{true, []string{noOpAllBs}},
		// Do not allow peers to appear before FS
		{false, []string{peersBs, fsBs, commitLogBs, noOpNoneBs}},
		// Do not allow a non-data fetching bootstrapper twice
		{false, []string{commitLogBs, noOpAllBs, noOpNoneBs}},
		// Do not allow multiple bootstrappers to appear
		{false, []string{commitLogBs, commitLogBs, noOpNoneBs}},
		// Do not allow unknown bootstrappers
		{false, []string{"foo"}},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%v", tt.bootstrappers)
		t.Run(name, func(t *testing.T) {
			err := ValidateBootstrappersOrder(tt.bootstrappers)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
