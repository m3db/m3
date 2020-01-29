package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/x/config/configflag"
)

const (
	defaultPath = "/api/v1/services/m3db/placement"
)

type PlacementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        configflag.FlagStringSlice
	newNodeFlag     configflag.FlagStringSlice
	replaceFlag     configflag.FlagStringSlice
}

type XFlagSet struct {
	Flagset *flag.FlagSet
}
type XPlacementFlags struct {
	Args      *PlacementArgs
	finalArgs placementHandler
	//Globals   []string
	Placement *flag.FlagSet
	Add       *flag.FlagSet
	Delete    *flag.FlagSet
	Init      *flag.FlagSet
	Replace   *flag.FlagSet
}
type placementHandler struct {
	add func(PlacementArgs, string)
	delete func(PlacementArgs, string)
	xget func(PlacementArgs, string)
	xinit func(PlacementArgs, string)
	replace func(PlacementArgs, string)
}

func SetupFlags() XPlacementFlags {
	return _setupFlags(
		&PlacementArgs{},
		placementHandler{
			add:     xadd,
			delete:  xdelete,
			xget:    xget,
			xinit:   xinit,
			replace: xreplace,
		},
	)
}
func _setupFlags(finalArgs *PlacementArgs, handler placementHandler) XPlacementFlags {

	placementFlags := flag.NewFlagSet("pl", flag.ExitOnError)
	deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	addFlags := flag.NewFlagSet("add", flag.ExitOnError)
	initFlags := flag.NewFlagSet("init", flag.ExitOnError)
	replaceFlags := flag.NewFlagSet("replace", flag.ExitOnError)

	finalArgs.deletePlacement = deleteFlags.Bool("all", false, "delete the entire placement")
	finalArgs.deleteNode = deleteFlags.String("node", "", "delete the specified node in the placement")
	initFlags.Var(&finalArgs.initFlag, "f", "initialize a placement. Specify a yaml file.")
	addFlags.Var(&finalArgs.newNodeFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
	replaceFlags.Var(&finalArgs.replaceFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
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

`, placementFlags.Name(), placementFlags.Name(), deleteFlags.Name(), addFlags.Name(), initFlags.Name(), replaceFlags.Name())
		placementFlags.PrintDefaults()
	}
	return XPlacementFlags{
		Args: finalArgs,
		finalArgs: handler,
		//Globals:   nil,
		Placement: placementFlags,
		Add:       addFlags,
		Delete:    deleteFlags,
		Init:      initFlags,
		Replace:   replaceFlags,
	}
}

func (xflags XPlacementFlags) ParseAndDo(cli []string, ep string) {
	// right here args should be like "pl delete -node someName"
	if len(cli) < 1 {
		xflags.Placement.Usage()
		os.Exit(1)
	}
	// pop and parse
	if err := dispatcher(cli[1:], xflags, ep); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

}

func dispatcher(inArgs []string, xflags XPlacementFlags, ep string) error {
	if err := xflags.Placement.Parse(inArgs); err != nil {
		xflags.Placement.Usage()
		return &errors.FlagsError{}
	}
	if xflags.Placement.NArg() == 0 {
		xflags.finalArgs.xget(*xflags.Args, ep)
		return nil
	}
	nextArgs := xflags.Placement.Args()
	switch nextArgs[0] {
	case xflags.Add.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Add); err != nil {
			return err
		}
		xflags.finalArgs.add(*xflags.Args, ep)
		return nil
	case xflags.Delete.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Delete); err != nil {
			return err
		}
		xflags.finalArgs.delete(*xflags.Args, ep)
		return nil
	case xflags.Init.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Init); err != nil {
			return err
		}
		xflags.finalArgs.xinit(*xflags.Args, ep)
		return nil
	case xflags.Replace.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Replace); err != nil {
			return err
		}
		xflags.finalArgs.replace(*xflags.Args, ep)
		return nil
	case "":
		xflags.finalArgs.xget(*xflags.Args, ep)
		return nil
	default:
		return &errors.FlagsError{}
	}

}
