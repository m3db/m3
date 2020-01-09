package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
	"os"
)

const (
	placementCommandName = "pl"

	// flag names
	DeleteAllName   = "deleteAll"
	DeleteNodeName  = "deleteNode"
	InitName        = "init"
	NewNodeName     = "newNode"
	ReplaceNodeName = "replaceNode"
)

type PlacementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        configflag.FlagStringSlice
	newNodeFlag     configflag.FlagStringSlice
	replaceFlag     configflag.FlagStringSlice
}

func SetupFlags(flags *PlacementArgs) *flag.FlagSet {
	placementFlags := flag.NewFlagSet(placementCommandName, flag.ExitOnError)
	flags.deletePlacement = placementFlags.Bool(DeleteAllName, false, "delete all instances in the placement")
	flags.deleteNode = placementFlags.String(DeleteNodeName, "", "delete the specified node in the placement")
	placementFlags.Var(&flags.initFlag, InitName, "initialize a placement. Specify a yaml file.")
	placementFlags.Var(&flags.newNodeFlag, NewNodeName, "add a new node to the placement. Specify the filename of the yaml.")
	placementFlags.Var(&flags.replaceFlag, ReplaceNodeName, "add a new node to the placement. Specify the filename of the yaml.")
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

The subcommand "%s" provides the ability to:

* delete an entire placement from a node
* remove a node from a placement
* add a new node to as existing placement
* replace a node within an existing placement
* initialize a placement

Default behaviour (no arguments) is to provide a json dump of the existing placement.

New node creation and node replacement require specification of
the desired placement parameters, which you are to provide via a yaml
file, the pathname of which is the argument for the cli option.

Specify only one action at a time.

Usage of %s:

`, placementFlags.Name(), placementFlags.Name(), placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return placementFlags
}
