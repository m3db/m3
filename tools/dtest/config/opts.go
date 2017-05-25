package config

import (
	"fmt"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/spf13/cobra"
)

// CLIOpts represents the CLI options to be set during a dtest
type CLIOpts struct {
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
}

// RegisterFlags registers all the common flags
func (c *CLIOpts) RegisterFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()
	pf.StringVarP(&c.M3DBBuildPath, "m3db-build", "b", "", "M3DB Binary")
	pf.StringVarP(&c.M3DBConfigPath, "m3db-config", "f", "", "M3DB Configuration File")
	pf.StringVarP(&c.M3EMConfigPath, "m3em-config", "c", "", "M3EM Configuration File")
	pf.BoolVarP(&c.SessionOverride, "session-override", "s", true, "Session Override")
	pf.StringVarP(&c.SessionToken, "session-token", "t", "dtest", "Session Token")
	pf.IntVarP(&c.NumNodes, "num-nodes", "n", 0, "Num Nodes to use in DTest")
}

// Validate validates the set options
func (c *CLIOpts) Validate() error {
	var me xerrors.MultiError
	if c.M3DBBuildPath == "" {
		me = me.Add(fmt.Errorf("m3db-build not specified"))
	}
	if c.M3DBConfigPath == "" {
		me = me.Add(fmt.Errorf("m3db-config not specified"))
	}
	if c.M3EMConfigPath == "" {
		me = me.Add(fmt.Errorf("m3em-config not specified"))
	}
	if c.SessionToken == "" {
		me = me.Add(fmt.Errorf("session-token not specified"))
	}
	return me.FinalError()
}
