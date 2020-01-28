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
	//Args      *PlacementArgs
	finalArgs *PlacementArgs
	Globals   []string
	Placement *flag.FlagSet
	Add       *flag.FlagSet
	Delete    *flag.FlagSet
	Init      *flag.FlagSet
	Replace   *flag.FlagSet
}
//type PlacementFlags struct {
//	Placement     *flag.FlagSet
//	placementDoer func(*PlacementArgs, string)
//	Delete        *flag.FlagSet
//	deleteDoer    func(*PlacementArgs, string)
//	Add           *flag.FlagSet
//	addDoer       func(*PlacementArgs, string)
//	Init          *flag.FlagSet
//	initDoer      func(*PlacementArgs, string)
//	Replace       *flag.FlagSet
//	replaceDoer   func(*PlacementArgs, string)
//}

//func SetupFlags(flags *PlacementArgs) PlacementFlags {
func SetupFlags(finalArgs *PlacementArgs) XPlacementFlags {
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

Usage of %s:

`, placementFlags.Name(), placementFlags.Name(), deleteFlags.Name(), addFlags.Name(), initFlags.Name(), replaceFlags.Name(), placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	//return PlacementFlags{
	//	Placement:     placementFlags,
	//	placementDoer: Get,
	//	Delete:        deleteFlags,
	//	deleteDoer:    Delete,
	//	Add:           addFlags,
	//	addDoer:       Add,
	//	Init:          initFlags,
	//	initDoer:      Init,
	//	Replace:       replaceFlags,
	//	replaceDoer:   Replace,
	//}
	return XPlacementFlags{
		//Args:      nil,
		finalArgs: finalArgs,
		Globals:   nil,
		Placement: placementFlags,
		Add:       addFlags,
		Delete:    deleteFlags,
		Init:      initFlags,
		Replace:   replaceFlags,
	}
}

func (xflags XPlacementFlags) ParseAndDo(cli []string, args *PlacementArgs, ep string) {
	//originalArgs := flag.Args()
	// right here args should be like "ns delete -name someName"
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
		xflags.xget(ep)
		return nil
	}
	//if err := checkArgs.CheckPerCase(inArgs, xflags.Placement); err != nil {
	//	return err
	//}
	nextArgs := xflags.Placement.Args()
	switch nextArgs[0] {
	case xflags.Add.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Add); err != nil {
			return err
		}
		xflags.add(ep)
		return nil
	case xflags.Delete.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Delete); err != nil {
			return err
		}
		xflags.delete(ep)
		return nil
	case xflags.Init.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Init); err != nil {
			return err
		}
		xflags.xinit(ep)
		return nil
	case xflags.Replace.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], xflags.Replace); err != nil {
			return err
		}
		xflags.replace(ep)
		return nil
	case "":
		xflags.xget(ep)
		return nil
	default:
		return &errors.FlagsError{}
	}

}
