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

	xerrors "github.com/m3db/m3x/errors"

	"github.com/spf13/cobra"
)

// Args represents the CLI arguments to be set during a dtest
type Args struct {
	// NodeBuildPath specifies the local fs path to the m3db binary
	NodeBuildPath string

	// NodeConfigPath specifies the local fs path to the m3db configuration
	NodeConfigPath string

	// DTestConfigPath specifies the local fs path to the m3em configuration
	DTestConfigPath string

	// NumNodes specifies the number of nodes to use from the m3em configuration
	NumNodes int

	// SessionToken specifies the token used during remote operations
	SessionToken string

	// SessionOverride specifies if exisiting dtests maybe overridden on remote
	// agents
	SessionOverride bool

	// InitialReset specifies if a Teardown() call should be made to remote
	// agents before running a test. It's useful to reset a running agent
	// in the event of an earlier run crashing.
	InitialReset bool
}

// RegisterFlags registers all the common flags
func (a *Args) RegisterFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVarP(&a.NodeBuildPath, "m3db-build", "b", "", "M3DB Binary")
	pf.StringVarP(&a.NodeConfigPath, "m3db-config", "f", "", "M3DB Configuration File")
	pf.StringVarP(&a.DTestConfigPath, "dtest-config", "d", "", "DTest Configuration File")
	pf.BoolVarP(&a.SessionOverride, "session-override", "o", false, "Session Override")
	pf.StringVarP(&a.SessionToken, "session-token", "t", "dtest", "Session Token")
	pf.IntVarP(&a.NumNodes, "num-nodes", "n", 0, "Num Nodes to use in DTest")
	pf.BoolVarP(&a.InitialReset, "initial-reset", "r", false, "Initial Reset")
}

// Validate validates the set options
func (a *Args) Validate() error {
	var me xerrors.MultiError
	if a.NodeBuildPath == "" {
		me = me.Add(fmt.Errorf("m3db-build not specified"))
	}
	if a.NodeConfigPath == "" {
		me = me.Add(fmt.Errorf("m3db-config not specified"))
	}
	if a.DTestConfigPath == "" {
		me = me.Add(fmt.Errorf("dtest-config not specified"))
	}
	if a.SessionToken == "" {
		me = me.Add(fmt.Errorf("session-token not specified"))
	}
	return me.FinalError()
}
