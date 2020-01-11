package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
	"os"
)

type PlacementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        configflag.FlagStringSlice
	newNodeFlag     configflag.FlagStringSlice
	replaceFlag     configflag.FlagStringSlice
}

type PlacementFlags struct {
	Placement *flag.FlagSet
	Delete    *flag.FlagSet
	Add       *flag.FlagSet
	Init      *flag.FlagSet
	Replace   *flag.FlagSet
}

func SetupFlags(flags *PlacementArgs) PlacementFlags {
	placementFlags := flag.NewFlagSet("pl", flag.ExitOnError)
	deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	addFlags := flag.NewFlagSet("add", flag.ExitOnError)
	initFlags := flag.NewFlagSet("init", flag.ExitOnError)
	replaceFlags := flag.NewFlagSet("replace", flag.ExitOnError)

	flags.deletePlacement = deleteFlags.Bool("all", false, "delete the entire placement")
	flags.deleteNode = deleteFlags.String("node", "", "delete the specified node in the placement")
	initFlags.Var(&flags.initFlag, "f", "initialize a placement. Specify a yaml file.")
	addFlags.Var(&flags.newNodeFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
	replaceFlags.Var(&flags.replaceFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

The placements subcommand "%s" provides the ability to:

* list the info for the placement
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

It has the following subcommands:
	%s
	%s
	%s
	%s

Usage of %s:

`, placementFlags.Name(), placementFlags.Name(), deleteFlags.Name(), addFlags.Name(), initFlags.Name(), replaceFlags.Name(), placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return PlacementFlags{Placement: placementFlags, Delete: deleteFlags, Add: addFlags, Init: initFlags, Replace: replaceFlags}
}
