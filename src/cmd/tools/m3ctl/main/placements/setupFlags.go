package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
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

func ParseAndDo(args *PlacementArgs, flags *PlacementFlags, ep string, log *zap.SugaredLogger) {
	if err := flags.Placement.Parse(flag.Args()[1:]); err != nil {
		flags.Placement.Usage()
		os.Exit(1)
	}
	if flags.Placement.NArg() == 0 {
		Get(args, ep, log)
		os.Exit(0)
	}
	switch flag.Arg(1) {
	case flags.Add.Name():
		if err := flags.Add.Parse(flag.Args()[2:]); err != nil {
			flags.Add.Usage()
			os.Exit(1)
		}
		if flags.Add.NFlag() == 0 {
			flags.Add.Usage()
			os.Exit(1)
		}
		flags.Add.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					flags.Add.Usage()
					os.Exit(1)
				}
			}
		})
		Add(args, ep, log)
	case flags.Delete.Name():
		if err := flags.Delete.Parse(flag.Args()[2:]); err != nil {
			flags.Delete.Usage()
			os.Exit(1)
		}
		if flags.Delete.NFlag() == 0 {
			flags.Delete.Usage()
			os.Exit(1)
		}
		flags.Delete.Visit(func(f *flag.Flag) {
			if len(f.Value.String()) == 0 {
				fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
				flags.Delete.Usage()
				os.Exit(1)
			}
		})
		Delete(args, ep, log)
	case flags.Init.Name():
		if err := flags.Init.Parse(flag.Args()[2:]); err != nil {
			flags.Init.Usage()
			os.Exit(1)
		}
		if flags.Init.NFlag() == 0 {
			flags.Init.Usage()
			os.Exit(1)
		}
		flags.Init.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					flags.Init.Usage()
					os.Exit(1)
				}
			}
		})
		Init(args, ep, log)
	case flags.Replace.Name():
		if err := flags.Replace.Parse(flag.Args()[2:]); err != nil {
			flags.Replace.Usage()
			os.Exit(1)
		}
		if flags.Replace.NFlag() == 0 {
			flags.Replace.Usage()
			os.Exit(1)
		}
		flags.Replace.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					flags.Replace.Usage()
					os.Exit(1)
				}
			}
		})
		Replace(args, ep, log)
	default:
		Get(args, ep, log)
	}

}
