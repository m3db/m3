package config

import (
	"fmt"

	"github.com/spf13/cobra"

	xerrors "github.com/m3db/m3x/errors"
)

// Args represents the CLI arguments to be set during a dtest
type Args struct {
	// M3DBBuildPath specifies the local fs path to the m3db binary
	M3DBBuildPath string

	// M3DBConfigPath specifies the local fs path to the m3db configuration
	M3DBConfigPath string

	// M3EMConfigPath specifies the local fs path to the m3em configuration
	M3EMConfigPath string

	// NumNodes specifies the number of nodes to use from the m3em configuration
	NumNodes int

	// SessionOverride specifies if exisiting dtests maybe overriden on remote
	// agents
	SessionOverride bool

	// SessionToken specifies the token used during remote operations
	SessionToken string

	// InitialReset specifies if a Teardown() call should be made to remote
	// agents before running a test. It's useful to reset a running agent
	// in the event of an earlier run crashing.
	InitialReset bool
}

// RegisterFlags registers all the common flags
func (a *Args) RegisterFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVarP(&a.M3DBBuildPath, "m3db-build", "b", "", "M3DB Binary")
	pf.StringVarP(&a.M3DBConfigPath, "m3db-config", "f", "", "M3DB Configuration File")
	pf.StringVarP(&a.M3EMConfigPath, "m3em-config", "c", "", "M3EM Configuration File")
	pf.BoolVarP(&a.SessionOverride, "session-override", "s", true, "Session Override")
	pf.StringVarP(&a.SessionToken, "session-token", "t", "dtest", "Session Token")
	pf.IntVarP(&a.NumNodes, "num-nodes", "n", 0, "Num Nodes to use in DTest")
	pf.BoolVarP(&a.InitialReset, "initial-reset", "r", false, "Initial Reset")
}

// Validate validates the set options
func (a *Args) Validate() error {
	var me xerrors.MultiError
	if a.M3DBBuildPath == "" {
		me = me.Add(fmt.Errorf("m3db-build not specified"))
	}
	if a.M3DBConfigPath == "" {
		me = me.Add(fmt.Errorf("m3db-config not specified"))
	}
	if a.M3EMConfigPath == "" {
		me = me.Add(fmt.Errorf("m3em-config not specified"))
	}
	if a.SessionToken == "" {
		me = me.Add(fmt.Errorf("session-token not specified"))
	}
	return me.FinalError()
}
