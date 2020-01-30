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

// all the values from the cli args are stored in here
// for all the placement-related commands
type placementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        configflag.FlagStringSlice
	newNodeFlag     configflag.FlagStringSlice
	replaceFlag     configflag.FlagStringSlice
}

// this has all that the dispatcher needs to parse the cli
type Context struct {
	args      *placementArgs
	finalArgs placementHandler
	//Globals   []string
	Placement *flag.FlagSet
	Add       *flag.FlagSet
	Delete    *flag.FlagSet
	Init      *flag.FlagSet
	Replace   *flag.FlagSet
}
type placementHandler struct {
	add func(placementArgs, string)
	delete func(placementArgs, string)
	xget func(placementArgs, string)
	xinit func(placementArgs, string)
	replace func(placementArgs, string)
}

func InitializeFlags() Context {
	return _setupFlags(
		&placementArgs{},
		placementHandler{
			add:     xadd,
			delete:  xdelete,
			xget:    xget,
			xinit:   xinit,
			replace: xreplace,
		},
	)
}
func _setupFlags(finalArgs *placementArgs, handler placementHandler) Context {

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
	return Context{
		args: finalArgs,
		finalArgs: handler,
		//Globals:   nil,
		Placement: placementFlags,
		Add:       addFlags,
		Delete:    deleteFlags,
		Init:      initFlags,
		Replace:   replaceFlags,
	}
}

func (ctx Context) ParseAndDo(cli []string, ep string) error {
	// right here args should be like "pl delete -node someName"
	if len(cli) < 1 {
		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Placement.Parse(inArgs); err != nil {
		ctx.Placement.Usage()
		return  &errors.FlagsError{}
	}
	if ctx.Placement.NArg() == 0 {
		ctx.finalArgs.xget(*ctx.args, ep)
		return nil
	}

	// pop and dispatch
	if err := dispatcher(ctx, ep); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return err
	}

	return nil

}

func dispatcher(ctx Context, ep string) error {
	nextArgs := ctx.Placement.Args()
	switch nextArgs[0] {
	case ctx.Add.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], ctx.Add); err != nil {
			return err
		}
		ctx.finalArgs.add(*ctx.args, ep)
		return nil
	case ctx.Delete.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], ctx.Delete); err != nil {
			return err
		}
		ctx.finalArgs.delete(*ctx.args, ep)
		return nil
	case ctx.Init.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], ctx.Init); err != nil {
			return err
		}
		ctx.finalArgs.xinit(*ctx.args, ep)
		return nil
	case ctx.Replace.Name():
		if err := checkArgs.CheckPerCase(nextArgs[1:], ctx.Replace); err != nil {
			return err
		}
		ctx.finalArgs.replace(*ctx.args, ep)
		return nil
	case "":
		ctx.finalArgs.xget(*ctx.args, ep)
		return nil
	default:
		return &errors.FlagsError{}
	}

}
