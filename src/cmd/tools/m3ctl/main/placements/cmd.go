package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/x/config/configflag"
	"os"
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

type PlacementFlags struct {
	Placement     *flag.FlagSet
	PlacementDoer func(*PlacementArgs, string)
	Delete        *flag.FlagSet
	DeleteDoer    func(*PlacementArgs, string)
	Add           *flag.FlagSet
	AddDoer       func(*PlacementArgs, string)
	Init          *flag.FlagSet
	InitDoer      func(*PlacementArgs, string)
	Replace       *flag.FlagSet
	ReplaceDoer   func(*PlacementArgs, string)
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
	return PlacementFlags{
		Placement:     placementFlags,
		PlacementDoer: Get,
		Delete:        deleteFlags,
		DeleteDoer:    Delete,
		Add:           addFlags,
		AddDoer:       Add,
		Init:          initFlags,
		InitDoer:      Init,
		Replace:       replaceFlags,
		ReplaceDoer:   Replace,
	}
}

func ParseAndDo(args *PlacementArgs, flags *PlacementFlags, ep string) {
	originalArgs := flag.Args()
	// right here args should be like "ns delete -name someName"
	if len(originalArgs) < 1 {
		flags.Placement.Usage()
		os.Exit(1)
	}
	// pop and parse
	if err := parseAndDo(originalArgs[1:], args, flags, ep); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

}

func parseAndDo(args []string, finalArgs *PlacementArgs, flags *PlacementFlags, ep string) error {
	if err := flags.Placement.Parse(args); err != nil {
		flags.Placement.Usage()
		return &errors.FlagsError{}
	}
	if flags.Placement.NArg() == 0 {
		flags.PlacementDoer(finalArgs, ep)
		return nil
	}
	nextArgs := flags.Placement.Args()
	switch nextArgs[0] {
	case flags.Add.Name():
		if err := flags.Add.Parse(nextArgs[1:]); err != nil {
			flags.Add.Usage()
			return &errors.FlagsError{}
		}
		if flags.Add.NFlag() == 0 {
			flags.Add.Usage()
			return &errors.FlagsError{}
		}
		flags.AddDoer(finalArgs, ep)
		return nil
	case flags.Delete.Name():
		if err := flags.Delete.Parse(nextArgs[1:]); err != nil {
			flags.Delete.Usage()
			return &errors.FlagsError{}
		}
		if flags.Delete.NFlag() == 0 {
			flags.Delete.Usage()
			return &errors.FlagsError{}
		}
		flags.DeleteDoer(finalArgs, ep)
		return nil
	case flags.Init.Name():
		if err := flags.Init.Parse(nextArgs[1:]); err != nil {
			flags.Init.Usage()
			return &errors.FlagsError{}
		}
		if flags.Init.NFlag() == 0 {
			flags.Init.Usage()
			return &errors.FlagsError{}
		}
		flags.InitDoer(finalArgs, ep)
		return nil
	case flags.Replace.Name():
		if err := flags.Replace.Parse(nextArgs[1:]); err != nil {
			flags.Replace.Usage()
			return &errors.FlagsError{}
		}
		if flags.Replace.NFlag() == 0 {
			flags.Replace.Usage()
			return &errors.FlagsError{}
		}
		flags.ReplaceDoer(finalArgs, ep)
		return nil
	case "":
		flags.PlacementDoer(finalArgs, ep)
		return nil
	default:
		return &errors.FlagsError{}
	}

}
